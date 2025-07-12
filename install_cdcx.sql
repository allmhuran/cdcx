:on error exit

-- Name of the database in which CDCX objects should be created.
-- The script will attempt to create this database if it does not exist.
:setvar CDCX_DB_NAME 

-- Name of the database containing the source data
:setvar SOURCE_DB_NAME 

-- name of the schema in which cdc extensions objects will be created
:setvar CDCX_SCHEMA_NAME cdcx

go


-- CDCX DB CREATION -----------------------------------------------------------------------------------------
if exists(select * from sys.databases where name = '$(CDCX_DB_NAME)') set noexec on;
go
create database [$(CDCX_DB_NAME)];
alter database [$(CDCX_DB_NAME)] set recovery simple;
alter database [$(CDCX_DB_NAME)] set trustworthy on;
go
set noexec off;
go

use [$(CDCX_DB_NAME)];
go

set xact_abort, nocount on;
go
declare @db sysname = db_name();
go

-- Table valued types are created before the deployment transaction to avoid self-deadlocks.
-- If deployment fails, these must be cleaned up separately!

if schema_id('$(CDCX_SCHEMA_NAME)') is not null set noexec on;
go
create schema [$(CDCX_SCHEMA_NAME)] authorization dbo;
go
set noexec off;
go

if type_id('$(CDCX_SCHEMA_NAME).smallintSet') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].smallintSet as table (v smallint primary key);
go
set noexec off;
go

if type_id('$(CDCX_SCHEMA_NAME).CdcColumnsOutput') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].CdcColumnsOutput as table (column_ordinal int, column_name sysname, key_ordinal int)
go
set noexec off;
go
 
if type_id('$(CDCX_SCHEMA_NAME).sysnameSet') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].sysnameSet as table (v sysname primary key);
go
set noexec off;
go



begin tran;
go

drop function if exists [$(CDCX_SCHEMA_NAME)].Changed;
drop table if exists [$(CDCX_SCHEMA_NAME)].integers;
drop function if exists [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals;
go

create table [$(CDCX_SCHEMA_NAME)].integers(i int primary key clustered);
insert      [$(CDCX_SCHEMA_NAME)].integers
select      top 65536 row_number() over (order by o1.object_id) - 1
from        sys.all_objects o1
cross join  sys.all_objects o2;
go

create or alter function [$(CDCX_SCHEMA_NAME)].SetBit(@bitPosition smallint, @mask varbinary(128))
returns varbinary(128) 
with schemabinding as
begin 
   -- in CDC the first column is position 1. Switch to 0-based to make the math simpler
   set @bitPosition -= 1;

   -- start position for sql substring function is 1-based
   declare @byteStart int = datalength(@mask) - (@bitPosition / 8);

   if(@byteStart > 0)
   begin
      declare @byte binary(1) = cast(substring(@mask, @byteStart, 1) as tinyint)
                                 | cast(power(2, @bitPosition % 8) as tinyint)

      set @mask = cast(stuff(@mask, @byteStart, 1, @byte) as varbinary(128));
   end;

   return @mask;
end;
go

create or alter function [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals(@columnOrdinals [$(CDCX_SCHEMA_NAME)].smallintSet readonly) 
returns varbinary(128)
with schemabinding
as
begin
   declare @mask binary(128) = 0x00;

   select   @mask = [$(CDCX_SCHEMA_NAME)].setBit(v, @mask)
   from     @columnOrdinals
   option   (maxdop 1);

   declare @maxOrdinal int = (select max(v) - 1 from @columnOrdinals);

   return cast(right(@mask, (@maxOrdinal / 8) + 1) as varbinary(128));
end
go

create or alter function [$(CDCX_SCHEMA_NAME)].Changed(@operation int, @updateMask varbinary(128), @checkBits varbinary(128))
returns table 
with schemabinding as
return
(
      select cdcx_deleted = iif(@operation = 1, 1, 0) where @operation in (1, 2)
      union all
      select      iif(@operation = 3, 1, 0)
      from        (values (cast(right(@updateMask, datalength(@checkBits)) as varbinary(128)))) t(mask)
      cross join  (select i from [$(CDCX_SCHEMA_NAME)].integers where i between 1 and datalength(@checkbits)) u
      cross apply (
                     select   cdc = cast(substring(mask, u.i, 1) as tinyint),
                              chk = cast(substring(@checkBits, u.i, 1) as tinyint)
                  ) bytes
      where       @operation in (3, 4)
                  and bytes.cdc & bytes.chk > 0
)               
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].GetCdcColumns
(
   @cdcDbName sysname, 
   @captureInstanceName sysname, 
   @ordinalsOnly bit = 1, 
   @columnNames [$(CDCX_SCHEMA_NAME)].sysNameSet readonly) as
begin
   declare @sql nvarchar(max) = N'
   use [' + @cdcDbName + '];
   select      cc.column_ordinal ' + iif(@ordinalsOnly = 1, '', ', cc.column_name, ic.key_ordinal') + '
   from        cdc.change_tables      ct
   join        cdc.captured_columns   cc  on cc.object_id = ct.object_id '
   + iif(not exists(select * from @columnNames), '', '
   join        @columnNames           cn  on cc.column_name = cn.v
   ') + iif(@ordinalsOnly = 1, '', '
   left join   sys.indexes            ix  on ix.object_id = ct.source_object_id
                                             and ix.name = ct.index_name
   left join   sys.index_columns      ic  on ic.index_id = ix.index_id
                                             and ic.object_id = ix.object_id
                                             and ic.column_id = cc.column_id') + '
   where       ct.capture_instance = @captureInstanceName collate database_default';

   exec sys.sp_executeSql @sql, N'@captureInstanceName sysname, @columnNames [$(CDCX_SCHEMA_NAME)].SysNameSet readonly', @captureInstanceName, @columnNames;
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[GetParams]
(
   @cdcDbName sysname,
   @captureInstanceName sysname,
   @columns [cdcx].sysnameSet readonly,
   @previousLastLsn binary(10),
   @mask varbinary(128) = null output,
   @nextStartLsn binary(10) = null output,
   @nextEndLsn binary(10) = null output,
   @changesMissed bit = 0 output
) with execute as 'dbo' as 
begin
   set nocount on;
   
   set @previousLastLsn = isnull(@previousLastLsn, 0x0);

   declare @minLsn binary(10);

   declare @sql nvarchar(max);

   set @sql = N'
   use [' + @cdcDbName + '];
   select   @minLsn        = sys.fn_cdc_get_min_lsn(@captureInstanceName), 
            @nextStartLsn  = sys.fn_cdc_increment_lsn(@previousLastLsn), 
            @nextEndLsn    = sys.fn_cdc_get_max_lsn();';

   exec sys.sp_executesql 
      @sql, 
      N'@captureInstanceName sysname, @previousLastLsn binary(10), @minLsn binary(10) output, @nextStartLsn binary(10) output, @nextEndLsn binary(10) output',
      @captureInstanceName, @previousLastLsn, @minLsn output, @nextStartLsn output, @nextEndLsn output;

   if (@nextStartLsn < @minLsn) select @changesMissed = 1, @nextStartLsn = @minLsn;
   else set @changesMissed = 0;
   
   if (exists(select * from @columns))
   begin

      declare @ordinals [$(CDCX_SCHEMA_NAME)].smallintSet;
      insert @ordinals exec [$(CDCX_SCHEMA_NAME)].GetCdcColumns @cdcDbName, @captureInstanceName, 1, @columns

      if (@@rowcount != (select count(*) from @columns)) throw 50001, 'At least one specified column was not found in the cdc source', 0; 
      
      set @mask = [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals(@ordinals);

   end
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Net](@cdcDb sysname, @captureInstanceName sysname) as
begin
   set nocount, xact_abort on;

   declare @cmd nvarchar(max);

   begin try

      begin tran;

      set @cmd = N'
create or alter function [$(CDCX_SCHEMA_NAME)].[<cdcdb.><instance>.net]
(
   @startLsn binary(10), 
   @endLsn binary(10), 
   @checkBits varbinary(128)
) 
returns table as
return
(
   select   changed.cdcx_deleted,
            <changed.changeColumns>
   from     (
               select      cdcx_rowNumber =  row_number() over
                                             (
                                                partition by <keyColumns>
                                                order by t.[__$start_lsn] desc, t.[__$seqval] desc, t.[__$operation] desc
                                             ),
                           cdcx_lastOp    =  first_value(t.[__$operation]) over 
                                             (
                                                partition by <keyColumns>
                                                order by t.[__$start_lsn] desc, t.[__$seqval] desc, t.[__$operation] desc
                                                rows unbounded preceding
                                             ),
                           cdcx_firstOp   =  first_value(t.[__$operation]) over 
                                             (
                                                partition by <keyColumns>
                                                order by t.[__$start_lsn] asc, t.[__$seqval] asc, t.[__$operation] desc
                                                rows unbounded preceding
                                             ),
                           c.cdcx_deleted,
                           <t.changeColumns>            
               from        <[cdcdb].>[cdc].[<instance>_ct] t
               cross apply $(CDCX_SCHEMA_NAME).Changed(t.[__$operation], t.[__$update_mask], @checkBits) c
               where       t.[__$start_lsn] between @startLsn and @endLsn                        
            ) changed
            where    changed.cdcx_rowNumber = 1
                     and not (changed.cdcx_lastOp = 2 and changed.cdcx_firstOp = 1)
);';

      declare 
         @cols [$(CDCX_SCHEMA_NAME)].CdcColumnsOutput,
         @keyColumns nvarchar(max) = N'', 
         @separator nvarchar(4) = N', ',
         @changeColumns nvarchar(max) = N'';

      insert @cols exec [$(CDCX_SCHEMA_NAME)].GetCdcColumns '$(SOURCE_DB_NAME)', @captureInstanceName, 0, default;


      select   @keyColumns += quotename(column_name) + @separator
      from     @cols
      where    key_ordinal is not null
      order by key_ordinal asc
      option   (maxdop 1);      

      set @keyColumns = left(@keyColumns, len(@keyColumns) - len(@separator));

      
      select   @changeColumns += '<$>.' + quotename(column_name) + @separator
      from     @cols
      order by column_ordinal asc
      option   (maxdop 1);

      set @changeColumns = left(@changeColumns, len(@changeColumns) - len(@separator));

      set @cmd = replace(@cmd, '<keyColumns>', @keyColumns);
      set @cmd = replace(@cmd, '<changed.changeColumns>', replace(@changeColumns, '<$>', 'changed'));
      set @cmd = replace(@cmd, '<t.changeColumns>', replace(@changeColumns, '<$>', 't'));
      set @cmd = replace(@cmd, '<cdcdb.>', iif(@cdcDb = db_name(), '', @cdcDb + '.'));
      set @cmd = replace(@cmd, '<[cdcdb].>', iif(@cdcDb = db_name(), '', quotename(@cdcDb) + '.'));
      set @cmd = replace(@cmd, '<instance>', @captureInstanceName);

      exec (@cmd);  

      commit;
      return 0;

   end try begin catch
      
      print cast('@cmd = ' + char(13) + char(10) + @cmd as varchar(max));
      throw;

   end catch
end
go


create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup](@cdcDatabaseName sysname, @captureInstanceName sysname) as
begin
   set nocount, xact_abort on;

   begin try

      begin tran;

      exec [$(CDCX_SCHEMA_NAME)].[Setup.Net] @cdcDatabaseName, @captureInstanceName;

      commit;
      return 0;

   end try begin catch
      
      if (@@trancount > 0) rollback;
      throw;

   end catch      
end
go



commit
go

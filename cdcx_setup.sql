--exec cdcx.[setup.uninstall] 'cdcx'

-- CDCX SETUP
:on error exit

-- Name of the database in which CDCX objects should be created.
-- The script will attempt to create this database if it does not exist.
:setvar CDCX_DB_NAME 

-- name of the schema in which cdc extensions objects will be created
:setvar CDCX_SCHEMA_NAME 


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CDCX DB CREATION 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if exists(select * from sys.databases where name = '$(CDCX_DB_NAME)') set noexec on;
go
create database [$(CDCX_DB_NAME)];
go
set noexec off;
go

use [$(CDCX_DB_NAME)];
go

set xact_abort, nocount on;
go

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Table valued types are created in a separate transaction avoid self-deadlocks. If deployment fails, these must be cleaned up separately!
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

begin tran
go

if schema_id('$(CDCX_SCHEMA_NAME)') is not null set noexec on;
go
create schema [$(CDCX_SCHEMA_NAME)] authorization dbo;
go
set noexec off;
go

if type_id('$(CDCX_SCHEMA_NAME).SmallintSet') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].SmallintSet as table (v smallint primary key);
go
set noexec off;
go

if type_id('$(CDCX_SCHEMA_NAME).CdcColumnsOutput') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].CdcColumnsOutput as table (column_ordinal int, column_name sysname, key_ordinal int)
go
set noexec off;
go
 
if type_id('$(CDCX_SCHEMA_NAME).SysnameSet') is not null set noexec on;
go
create type [$(CDCX_SCHEMA_NAME)].SysnameSet as table (v sysname primary key);
go
set noexec off;
go

commit;
go

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- all CDCX objecs other than table valued types
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

begin tran;
go

-- clean up legacy objects

drop function if exists [$(CDCX_SCHEMA_NAME)].Changed
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[Setup.Diff];
drop function if exists [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals;
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[sys.Setup.Net];
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[sys.Setup.Diff];
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[sys.Setup];
drop procedure if exists [$(CDCX_SCHEMA_NAME)].LongPrint;
drop procedure if exists [$(CDCX_SCHEMA_NAME)].PrintLong;
drop function if exists [$(CDCX_SCHEMA_NAME)].LongString;
drop procedure if exists [$(CDCX_SCHEMA_NAME)].GetCdcColumns;
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[sys.AddCaptureInstanceColumns];
drop procedure if exists [$(CDCX_SCHEMA_NAME)].[AddCaptureInstanceColumns];
drop procedure if exists [$(CDCX_SCHEMA_NAME)].GetParams;
drop table if exists [$(CDCX_SCHEMA_NAME)].Integers;
go

create or alter procedure  [$(CDCX_SCHEMA_NAME)].[Setup.Uninstall](@cdcxSchemaName sysname = null) as
begin
   set xact_abort, nocount on;

   begin try

      declare c cursor local fast_forward for 
      select    concat
               (
                  'drop ',
                  case type
                     when 'P' then 'procedure '
                     when 'V' then 'view '
                     when 'FN' then 'function '
                     when 'IF' then 'function '
                     when 'SN' then 'synonym '
                     when 'U' then 'table '
                  end,
                  ' if exists ',
                  quotename(@cdcxSchemaName),
                  '.',
                  quotename(o.name)
               )
      from     sys.all_objects   o
      join     sys.schemas       s on o.schema_id = s.schema_id
      where    o.type in ('P', 'V', 'FN', 'IF', 'SN', 'U')
               and s.name = @cdcxSchemaName
               and o.name != 'Setup.Uninstall'
      order by case type
                  when 'V' then 1
                  when 'FN' then 2
                  when 'IF' then 3
                  when 'P' then 4
                  when 'SN' then 5
                  when 'U' then 6
               end asc;

      declare @ddl nvarchar(2048);
      open c;
      fetch next from c into @ddl;
      while (@@fetch_status = 0)
      begin
         begin try
            exec (@ddl);
            print @ddl;
         end try begin catch
         end catch         
         fetch next from c into @ddl;
      end
      close c;

      open c;
      fetch next from c into @ddl;
      while (@@fetch_status = 0)
      begin
         begin try
            exec (@ddl);
            print @ddl;
         end try begin catch
         end catch         
         fetch next from c into @ddl;
      end
      close c;

      return 0;

   end try begin catch

      if (@@trancount > 0) rollback;
      throw;

   end catch
end
go

create table [$(CDCX_SCHEMA_NAME)].Integers(i int primary key clustered);
insert      [$(CDCX_SCHEMA_NAME)].Integers
select      top 2048 row_number() over (order by o1.object_id) - 1
from        sys.all_objects o1
cross join  sys.all_objects o2;
go

create or alter function [$(CDCX_SCHEMA_NAME)].XmlString(@string varchar(max)) 
returns xml as
begin
/*
   select a long string as formatted xml, provides a way of outputting long strings (like ddl) that exceed print() length
*/
   return
   (   
      select [processing-instruction(statement)] = ':' + char(10) + @string + char(10) + char(10) for xml path(''), type
   )
end
go

create or alter function [$(CDCX_SCHEMA_NAME)].SetBit(@bitPosition smallint, @mask varbinary(128))
returns varbinary(128) 
with schemabinding as
/*
   Given some bitmask @mask, set the bit at position @bitPosition to 1
*/
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

create or alter function [CDCX].[Split](@string nvarchar(max), @separator nvarchar(255)) returns table as
/*
   Given an input string and a separator, split the string on the separator, producing ordered output.
   Used by CDCX for compatibility with databases at compatibility level < 130
*/
return 
(
	select		stringIndex    =  row_number() over (order by i),
					firstCharIndex =  i + ((row_number() over (order by i) - 1) * (len(@separator) - 1)),
					string         =  ltrim
                                 (
                                    rtrim
                                    (
                                       substring
                                       (
                                          nchar(1) + replace(@string, @separator, nchar(1)) + nchar(1), 
                                          i + 1, 
                                          charindex
                                          (
                                             nchar(1), 
                                             nchar(1) + replace(@string, @separator, nchar(1)) + nchar(1),
                                             i + 1
                                          ) - (i + 1)
                                       )
                                    )
                                 )
		from		[cdcx].Integers
		where		i < len(replace(@string, @separator, nchar(0))) + 1
		         and substring(nchar(1) + replace(@string, @separator, nchar(1)) + nchar(1), i, 1) = nchar(1)
)
GO

create or alter function [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals(@columnOrdinals [$(CDCX_SCHEMA_NAME)].SmallintSet readonly) 
returns varbinary(128)
with schemabinding
as
begin
/*
   Set bits in a mask at the bit positions given by the @columnOrdinals table parameter, 
   truncate the mask to the byte length required according to the maximum column ordinal,
   and return mask.
*/
   declare @mask binary(128) = 0x00;

   select   @mask = [$(CDCX_SCHEMA_NAME)].SetBit(v, @mask)
   from     @columnOrdinals
   option   (maxdop 1);

   declare @maxOrdinal int = (select max(v) - 1 from @columnOrdinals);

   return cast(right(@mask, (@maxOrdinal / 8) + 1) as varbinary(128));
end
go

--create or alter function [$(CDCX_SCHEMA_NAME)].Changed(@operation int, @updateMask varbinary(128), @checkBits varbinary(128))
-- INCORPORATED INTO .CHANGES
--returns table
--with schemabinding as 
--/*
--   Returns a single row with a single column (cdcx_deleted, 1 or 0) if we care about the change represented by the operation, update mask, and checkbits.   
--   Otherwise will not return a row, and thus can be used as a filter.

--   This function is SOURCE AGNOSTIC since it accepts the cdc update mask as parameter. Once you're in here, it's just comparing one mask to another.

--   @updateMask is a representation of which columns actually changed. 
--   @checkBits is a representation of which columns we care about.
--   We need to bitwise-and these bit strings if the operation is an update.

--   Since only integer types (not binary types) can be bitwise-anded, we compare the relevant sections of the masks 
--   one byte at a time using a tally table to declaratively "iterate" over each byte and convert it to a tinyint.
--*/
--return
--(
--      select      cdcx_deleted = iif(@operation in (1, 3), 1, 0)
--      from        [$(CDCX_SCHEMA_NAME)].Integers ints
--      cross join  (values (datalength(@checkBits))) bits(length)
--      where       (
--                     @checkBits is null                                                                                          -- null filter => return all rows
--                     or 
--                     (
--                        ints.i between 1 and bits.length                                                                         -- no need to check bits that are outside of the bits we care about
--                        and
--                        (
--                           @operation in (1, 2)                                                                                  -- no need to check the masks for inserts or deletes
--                           or 
--                           (
--                              cast(substring(cast(right(@updateMask, bits.length) as varbinary(128)), ints.i, 1) as tinyint)     -- bitmasks are "right-aligned", but it's easier to taking substrings from the left, so align the bit fields on the left at the first relevant byte and get the byte
--                              & cast(substring(@checkBits, ints.i, 1) as tinyint)                                                -- the same byte from @checkBits
--                              > 0                                                                                                -- bitwise and of update mask byte and check byte > 0
--                           )
--                        )
--                     )                              
--                  )
--)
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.GetDbNameFromAlias](@dbAlias sysname, @dbName sysname output)
/*
   Given some alias being used by CDCX, what database is it referencing?
   This is determined by reading metadata for existing CDCX synonyms.
   We don't store any data anywhere in CDCX (other than the integers table, which is static and can be safely recreated at any time).
   This makes it easy to set up CDCX idempotently.
*/
as begin
   set nocount on;

   declare @count int, @err nvarchar(1024);

   select      @dbName  =  max(db.name),
               @count   =  count(distinct db.name)
   from        sys.synonyms   sy
   join        sys.schemas    sc on sc.schema_id = sy.schema_id
   cross apply (select isnull(parsename(sy.base_object_name, 3), db_name())) db(name)
   where       sc.name = '$(CDCX_SCHEMA_NAME)'               
               and sy.name like concat(@dbAlias, '.%');

   if (@count = 0) set @err = object_name(@@procid) + ':No database references found for alias "' + @dbAlias + '". Check sys.synonyms and/or re-run [$(CDCX_SCHEMA_NAME)].[Setup]';
   else if (@count > 1) set @err = object_name(@@procid) + ':Multiple database references found for alias "' + @dbAlias + '". Check sys.synonyms and/or re-run [$(CDCX_SCHEMA_NAME)].[Setup]';

   if (@err is not null) throw 50001, @err, 1;
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.ColumnOrdinals](@dbAlias sysname) as
begin
/*
   Geneates the cdcx .ColumnOrdinals function for a particular source database.
   The ColumnOrdinals function is used to generate bitmasks based on column ordinal positions for "columns you care about", 
   these bitmasks are compared to the cdc __$update_mask in the .Changed function.
   ColumnOrdinals is also used during setup to generate the ddl for the .Changes function.
*/
   set xact_abort, nocount on;

   declare @query nvarchar(max) = N'
create or alter function [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals]
(
   @schemaName sysname,
   @tableName sysname,
   @getKeyOrdinals bit,
   @columnNames [$(CDCX_SCHEMA_NAME)].SysnameSet readonly

) returns table as
return
(
   select      ct.capture_instance,
               ct.start_lsn,
               captureInstanceOrder = iif(ct.start_lsn = min(ct.start_lsn) over (), 1, 2),
               cc.column_name,
               cc.column_ordinal,               
               ic.key_ordinal,
               change_table = tt.name
   from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.change_tables]         ct
   join        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.tables]                ta on ta.object_id = ct.source_object_id
   join        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.schemas]               sc on sc.schema_id = ta.schema_id
   join        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.captured_columns]      cc on cc.object_id = ct.object_id
   left join   [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.indexes]               ix on @getKeyOrdinals = 1 and ix.object_id = ct.source_object_id and ix.name = ct.index_name
   left join   [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.index_columns]         ic on @getKeyOrdinals = 1 and ic.index_id = ix.index_id and ic.object_id = ix.object_id and ic.column_id = cc.column_id
   left join   [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.tables]                tt on tt.object_id = ct.object_id
   left join   @columnNames cn on cc.column_name = cn.v
   where       sc.name = @schemaName collate database_default
               and ta.name = @tableName collate database_default
               and (not exists (select * from @columnNames) or cc.column_name = cn.v)
   
)';

   set @query = replace(@query, '@dbAlias?', @dbAlias);
   exec (@query);
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.GetColumnExpressions]
/*
   Porduces various string expressions which are inserted into DDL templates during setup

   If there are two capture instances, this procedure also detects the value that should be used as the cutover LSN value when unioning data from both change tables.
*/
(
   @dbAlias sysname,
   @schemaName sysname,                               -- the source schema of the source table we are interested in
   @tableName sysname,                                -- the source table we are interested in
   @separator nvarchar(64),                           -- will be used to separate column names / expressions in all output parameters
   @instance1Expressions nvarchar(max) = null output, -- will contain the column expressions needed to select from capture instance 1 separated by @separator
   @instance2Expressions nvarchar(max) = null output, -- will contain the column expressions needed to select from capture instance 2 separated by @separator
   @cutoverLsn char(22) = null output,                -- will contain the cutover LSN to switch selection from capture instance 1 to capture instance 2 separated by @separator
   @updateMask1DataLength tinyint = null output,      -- will contain the data length in bytes of the __$update_mask for the first capture instance
   @updateMask2DataLength tinyint = null output,      -- will contain the data length in bytes of the __$update_mask for the second capture instance
   @keyColumnNames nvarchar(max) = null output,       -- will contain the names of key columns separated by @separator
   @nonKeyColumnNames nvarchar(max) = null output     -- will contain the names of non key columns separated by @separator
) as begin
   set nocount on
  
   set @instance1Expressions = N'';
   set @instance2Expressions = N'';   
   set @cutoverLsn = N'';
   set @keyColumnNames = N'';
   set @nonKeyColumnNames = N'';

   declare @sql nvarchar(max);
   

   -- get all the cdc metadata we need up front  ------------------------------------------------------------------------------------------------------------------------

   create table #columns (capture_instance sysname, start_lsn binary(10), captureInstanceOrder int, column_name sysname, column_ordinal int, key_ordinal int);

   set @sql = N'
      declare @c [$(CDCX_SCHEMA_NAME)].SysnameSet;

      insert      #columns(capture_instance, start_lsn, captureInstanceOrder, column_name, column_ordinal, key_ordinal)
      select      capture_instance, start_lsn, captureInstanceOrder, column_name, column_ordinal, key_ordinal
      from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals](@schemaName, @tableName, 1, @c)
      union all
      select      ''base'', null, null, c.name, null, null
      from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Tables]    t
      join        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Columns]   c on c.object_id = t.object_id
      join        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Schemas]   s on s.schema_id = t.schema_id
      where       s.name = @schemaName
                  and t.name = @tableName';

   set @sql = replace(@sql, '@dbAlias?', @dbAlias);   
   exec sp_executesql @sql, N'@schemaName sysname, @tableName sysname', @schemaName, @tableName;
  


   -- get the data lengths of the update masks ----------------------------------------------------------------------------------------------------------------------------

   select @updateMask1DataLength = ceiling(max(column_ordinal) / 8.0) from #columns where captureInstanceOrder = 1;
   select @updateMask2DataLength = ceiling(max(column_ordinal) / 8.0) from #columns where captureInstanceOrder = 2;



   -- generate the key column names ---------------------------------------------------------------------------------------------------------------------------------------

   select   @keyColumnNames += concat(@separator, quotename(column_name))
   from     (
               select   distinct 
                        column_name
               from     #columns 
               where    key_ordinal is not null
            ) t
   order by column_name asc
   option   (maxdop 1);

   set @keyColumnNames = stuff(@keyColumnNames, 1, len(@separator), ''); -- eliminate the leading separator

  

   -- generate non key column names ---------------------------------------------------------------------------------------------------------------------------------------
    
   select   @nonKeyColumnNames += concat(@separator, quotename(column_name))
   from     #columns
   where    capture_instance = 'base'
            and column_name not in (select column_name from #columns where key_ordinal is not null)
   order by column_name asc
   option   (maxdop 1);

   set @nonKeyColumnNames = stuff(@nonKeyColumnNames, 1, len(@separator), ''); -- eliminate the leading separator


  
   -- for all columns in the base tale, get the column name and whether or not it appears in each capture instance ---------------------------------------------------------

   create table #instanceColumns (column_name sysname primary key clustered, instance1 bit, instance2 bit);

   with allColumns as (select column_name from #columns where capture_instance = 'base')
   insert      #instanceColumns
   select      ac.column_name,
               iif(c1.column_name is null, 0, 1),
               iif(c2.column_name is null, 0, 1)
   from        allColumns  ac
   left join   #columns    c1 on c1.column_name = ac.column_name
                                 and c1.captureInstanceOrder = 1
   left join   #columns    c2 on c2.column_name = ac.column_name
                                 and c2.captureInstanceOrder = 2;
   

   -- get column expressions needed to read from capture instance 1 -------------------------------------------------------------------------------------------------------  

   select   @instance1Expressions +=   concat
                                       (
                                          @separator, 
                                          quotename(column_name),
                                          ' = ', 
                                          iif(ic.instance1 = 0, 'null', quotename(ic.column_name))                      
                                       )
   from     #instanceColumns ic
   option   (maxdop 1);

   set @instance1Expressions = stuff(@instance1Expressions, 1, len(@separator), ''); -- eliminate the leading separator

   -- if there is more than one capture instance, produce column expressions for the second instance as well as the cutover LSN as a hex formatted varchar -----------------

   if exists (select * from #instanceColumns where instance2 = 1)
   begin
      select @cutoverLsn = convert(varchar, max(start_lsn), 1) from #columns;

      set @instance2Expressions = N'';

      select   @instance2Expressions += concat
                                       (
                                          @separator,
                                          quotename(column_name),
                                          ' = ', 
                                          iif(ic.instance2 = 0, 'null', quotename(ic.column_name))  
                                       )
      from     #instanceColumns ic
      option   (maxdop 1);

      set @instance2Expressions = stuff(@instance2Expressions, 1, len(@separator), ''); -- eliminate the leading comma/newline
     
   end   
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.TableSynonyms](@dbAlias sysname, @schemaName sysname, @tableName sysname) as
/*
   Given some source table defined by @schemaName and @tableName in the database referenced by @dbAlias,
   Find the capture instance (or capture instances) being used by CDC to track that table.
   Then generate CDCX synonyms for those capture instances, as well as for the table itself.
   The CDCX synonyms always follow a consistent naming convention regardless of the actual capture instance name,
   For example, if the alias is "MyDb", the schema is "S", and the table is "T", and there are two capture isntances, then two synonyms will be generated:
      [$(CDCX_SCHEMA_NAME)].[MyDb.cdc.changeTables.S.T.1]
      [$(CDCX_SCHEMA_NAME)].[MyDb.cdc.changeTables.S.T.2]
*/
begin
   set xact_abort, nocount on;

   begin try;      

      begin tran;

      declare @dbName sysname;
      exec [$(CDCX_SCHEMA_NAME)].[Setup.GetDbNameFromAlias] @dbAlias, @dbName output;
      set @dbName = iif(@dbName = db_name(), '', quotename(@dbName) + '.');

      declare @ddl nvarchar(max) = N'
         declare @c [$(CDCX_SCHEMA_NAME)].SysnameSet;
         set @changeTable1 = (select top 1 change_table from [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals](@schemaName, @tableName, 0, @c) where captureInstanceOrder = 1);
         set @changeTable2 = (select top 1 change_table from [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals](@schemaName, @tableName, 0, @c) where captureInstanceOrder = 2);';

      set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);

      declare @changeTable1 sysname, @changeTable2 sysname;
      exec sp_executeSql 
         @ddl, 
         N'@schemaName sysname, @tableName sysname, @changeTable1 sysname output, @changeTable2 sysname output', 
         @schemaName, @tableName, @changeTable1 output, @changeTable2 output;   

      set @ddl = N'
         drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?];
         drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.1];
         drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.2];
         create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?] for @dbName?[@schemaName?].[@tableName?];
         create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.1] for @dbName?cdc.[@changeTable1?];
      ';

      if (@changeTable2 is not null) set @ddl += N'create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.2] for @dbName?cdc.[@changeTable2?];';  

      set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);
      set @ddl = replace(@ddl, '@dbName?', @dbName);
      set @ddl = replace(@ddl, '@schemaName?', @schemaName);
      set @ddl = replace(@ddl, '@tableName?', @tableName);
      set @ddl = replace(@ddl, '@changeTable1?', @changeTable1);
      if (@changeTable2 is not null) set @ddl = replace(@ddl, '@changeTable2?', @changeTable2);

      exec (@ddl);

      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      select setup_tableSynonyms_ddl = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch

end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Changes]
(
   @dbAlias sysname, 
   @schemaName sysname, 
   @tableName sysname, 
   @separator nvarchar(64),
   @instance1Expressions nvarchar(max),
   @instance2Expressions nvarchar(max),
   @cutoverLsn char(22),
   @updateMask1DataLength tinyint,
   @updateMask2DataLength tinyint
) as
/*
   Produce the cdcx .Changes function for a given source table in the database referenced by @dbAlias.
*/
begin
   set xact_abort, nocount on;

   exec [$(CDCX_SCHEMA_NAME)].[Setup.TableSynonyms] @dbAlias, @schemaName, @tableName;

   declare @ddl nvarchar(max) = N'
create or alter function [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?.Changes]
(
   @startLsn binary(10), 
   @endLsn binary(10),
   @mask1 varbinary(128) = null,
   @mask2 varbinary(128) = null
) returns table as 
/*
   Retuns changes from the cdc change table (or tables) tracking [@schemaName?].[@tableName?] in the source database.
   Unions both change tables together (if there are two), and, for each part of the untion, 
   and generates literal null output for any column that does not exist in the superset of all columns.   
   Uses a cutover LSN to determine which change table to pull from (if there is more than one).
   Up to two @mask values may be required, since each change table can have different ordinal positions for the same source column under some scenarios.
   The values to use for the @mask1 and @mask2 parameters are calculated for you by the [$(CDCX_SCHEMA_NAME)].[@dbAlias?.GetParams] stored procedure.
*/
return
(
   select      __$start_lsn,
               __$seqval,
               __$operation,
               cdcx_deleted = iif(__$operation in (1, 3), 1, 0),
               @instance1Expressions?
   from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.1]
   where       __$start_lsn between @startLsn and @endLsn
               and 
               (
                  @mask1 is null                   -- null filter => return all rows
                  or __$operation in (1, 2)        -- inserts and deletes always count  as a change
                  or exists                        -- there is a bit that is set in both the __$update_mask and @mask1
                  (
                     select   *
                     from     [$(CDCX_SCHEMA_NAME)].Integers ints
                     where    ints.i <= datalength(@mask1)
                              and
                              (
                                 cast(substring(__$update_mask, @updateMask1DataLength? - datalength(@mask1) + ints.i, 1) as tinyint) -- this update_mask is @updateMask1DataLength? bytes long
                                 & cast(substring(@mask1, ints.i, 1) as tinyint)   
                                 > 0
                              )                              
                  )
               )';

   if (@instance2Expressions > '')
   begin
      set @ddl += N'
               and __$start_lsn < @cutoverLsn?

   union all

   select      __$start_lsn,
               __$seqval,
               __$operation,
               cdcx_deleted = iif(__$operation in (1, 3), 1, 0),
               @instance2Expressions?
   from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.changeTables.@schemaName?.@tableName?.2]
   where       __$start_lsn between @startLsn and @endLsn
               and __$start_lsn >= @cutoverLsn?
               and
               (
                  @mask2 is null                   -- null filter => return all rows
                  or __$operation in (1, 2)        -- inserts and deletes always count as a change
                  or exists                        -- there is a bit that is set in both the __$update_mask and @mask2
                  (
                     select   *
                     from     [$(CDCX_SCHEMA_NAME)].Integers ints
                     where    ints.i <= datalength(@mask2)
                              and
                              (
                                 cast(substring(__$update_mask, @updateMask2DataLength? - datalength(@mask2) + ints.i, 1) as tinyint) -- this update_mask is @updateMask2DataLength? bytes long
                                 & cast(substring(@mask2, ints.i, 1) as tinyint)   
                                 > 0
                              )                              
                  )
               )';
   end

   set @ddl += '
);'

   set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);
   set @ddl = replace(@ddl, '@schemaName?', @schemaName);
   set @ddl = replace(@ddl, '@tableName?', @tableName);
   set @ddl = replace(@ddl, '@instance1Expressions?', replace(@instance1Expressions, @separator, N',' + nchar(10) + N'               '));
   set @ddl = replace(@ddl, '@updateMask1DataLength?', @updateMask1DataLength);
   if (@instance2Expressions > '')
   begin
      set @ddl = replace(@ddl, '@instance2Expressions?', replace(@instance2Expressions, @separator, N',' + nchar(10) + N'               '));
      set @ddl = replace(@ddl, '@cutoverLsn?', convert(varchar, @cutoverLsn, 1));
      set @ddl = replace(@ddl, '@updateMask2DataLength?', @updateMask2DataLength);
   end

   begin try

      begin tran;
      exec (@ddl);
      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      select failedDDL = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch

end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Net]
(
   @dbAlias sysname, 
   @schemaName sysname, 
   @tableName sysname,
   @separator nvarchar(64),
   @keyColumnNames nvarchar(max),
   @nonKeyColumnNames nvarchar(max)
) as
begin
/*
   Generates ddl for the cdcx net changes function for a particular source table. Called by cdcx setup
*/
   set nocount, xact_abort on;

   declare @ddl nvarchar(max);

   set @ddl = N'
create or alter function [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?.Net]
(
   @startLsn binary(10),
   @endLsn binary(10),
   @mask1 varbinary(128),
   @mask2 varbinary(128)   
)
returns table as
return
(
   select      cdcx_deleted = iif(lastOp.__$operation = 1, 1, 0),
               cdcx_firstStartLsn = firstOp.__$start_lsn,
               cdcx_firstSeqVal = firstOp.__$seqval,
               cdcx_firstOperation = firstOp.__$operation,
               @keyColumnNames?,
               @nonKeyColumnNames?
   from        (
                  -- distinct key values for any rows in the change tables which include a relevent change as determined by the filter masks
                  select      distinct 
                              @keyColumnNames?
                  from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?.Changes](@startLsn, @endLsn, @mask1, @mask2)
               ) keys
   cross apply (
                  -- the last operation in cdc in the specified window for the key, including the data columns we want. No need to filter by mask, we already did that for the keys
                  select      top 1 
                              __$start_lsn,
                              __$seqval,
                              __$operation,
                              @nonKeyColumnNames?
                  from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?.Changes](@startLsn, @endLsn, null, null)                
                  where       @where?                            
                  order by    __$start_lsn desc, 
                              __$seqval desc, 
                              __$operation desc
               ) lastOp
   cross apply (
                  -- The first operation in the specified window for the key, just the metadata columns. No need to filter by mask, we already did that for the keys
                  -- These columns allow a caller to join .Net back to .Changes to get the initial values for any columns they care about
                  select      top 1 
                              __$start_lsn, 
                              __$seqval, 
                              __$operation
                  from        [$(CDCX_SCHEMA_NAME)].[@dbAlias?.@schemaName?.@tableName?.Changes](@startLsn, @endLsn, null, null)
                  where       @where?
                              and __$start_lsn <= lastOp.__$start_lsn
                  order by    __$start_lsn asc, 
                              __$seqval asc, 
                              __$operation asc
               ) firstOp
               -- do not include rows where the first operation in the window is an insert and the last operation is a delete,
               -- since those rows might well have never existed!
   where       not (firstOp.__$operation = 2 and lastOp.__$operation = 1)
)';

   -- the where clause in the cross applies uses an equality predicate for all key columns. Generate that predicate

   declare @keyCols [$(CDCX_SCHEMA_NAME)].SysnameSet;
   insert   @keyCols
   select   string
   from     [$(CDCX_SCHEMA_NAME)].Split(@keyColumnNames, @separator);

   declare @where nvarchar(max) = N''

   select   @where += concat('and ', v, ' = keys.', v)
   from     @keyCols
   option   (maxdop 1);

   set @where = stuff(@where, 1, 4, '');

   -- replace template elements   

   set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);
   set @ddl = replace(@ddl, '@schemaName?', @schemaName);
   set @ddl = replace(@ddl, '@tableName?', @tableName);
   set @ddl = replace(@ddl, '@nonKeyColumnNames?', replace(@nonKeyColumnNames, @separator, N', '));
   set @ddl = replace(@ddl, '@keyColumnNames?', replace(@keyColumnNames, @separator, N', '));   
   set @ddl = replace(@ddl, '@where?', @where);

   begin try

      begin tran;
      exec (@ddl);  
      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      select failed_net_DDL = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch

end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.GetParams](@dbAlias sysname) as
begin
   set xact_abort, nocount on;

   declare @ddl nvarchar(max) = N'
create or alter procedure [$(CDCX_SCHEMA_NAME)].[@dbAlias?.GetParams]
(
   @schemaName sysname,
   @tableName sysname,
   @columns [$(CDCX_SCHEMA_NAME)].SysnameSet readonly,
   @previousEndLsn binary(10),
   @startLsn binary(10) = null output,
   @endLsn binary(10) = null output,
   @mask1 varbinary(128) = null output,
   @mask2 varbinary(128) = null output,
   @changesMissed bit = 0 output
) as
begin
/*
   Given a source capture instance and a previous high water mark,
   calculate the next lsn window that should be used to read change data.
   Clamp the window to a valid lsn range and indicate if changes have been missed.
   Also recalculate a cdcx @mask1 if a column list has been provided, and @mask2 for the second capture instance if there are two capture instances
*/
   set nocount on;
     
   declare @minLsn binary(10), @instanceCount tinyint;

   set @previousEndLsn = isnull(@previousEndLsn, 0x0);  

   select   @minLsn        =  min(start_lsn), 
            @startLsn      =  [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_increment_lsn](@previousEndLsn),
            @endLsn        =  [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_get_max_lsn](),
            @instanceCount =  count(distinct capture_instance)
   from     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.change_tables]   ct
   join     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.tables]          tb on tb.object_id = ct.source_object_id
   join     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.schemas]         sc on sc.schema_id = tb.schema_id
   where    sc.name = @schemaName
            and tb.name = @tableName;
      
   -- if the requested start lsn is prior to the min available then clamp the start to the min available and indicate that changes have been missed

   if (@startLsn < @minLsn) select @changesMissed = 1, @startLsn = @minLsn;
   else set @changesMissed = 0;

   -- if a set of columns has been provided, calculate new masks   

   if (exists(select * from @columns))
   begin
      declare @ordinals [$(CDCX_SCHEMA_NAME)].SmallintSet;

      insert   @ordinals
      select   column_ordinal
      from     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals](@schemaName, @tableName, 1, @columns) 
      where    captureInstanceOrder = 1;

      set @mask1 = [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals(@ordinals);

      -- if there is a second capture instance calculate a second mask, otherwise set the mask to null

      if (@instanceCount = 1) set @mask2 = null;
      else begin

         delete from @ordinals;

         insert   @ordinals
         select   column_ordinal
         from     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.ColumnOrdinals](@schemaName, @tableName, 1, @columns) 
         where    captureInstanceOrder = 2;

         set @mask2 = [$(CDCX_SCHEMA_NAME)].MaskFromOrdinals(@ordinals);
      end
   end
end';

   set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);
   
   begin try

      begin tran;
      exec (@ddl);
      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      select failed_setup_getparams = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.GetParamsByList](@dbAlias sysname) as
begin
   set xact_abort, nocount on;

   declare @ddl nvarchar(max) = N'
create or alter procedure [$(CDCX_SCHEMA_NAME)].[@dbAlias?.GetParamsByList]
(
   @schemaName sysname,
   @tableName sysname,
   @columnNameList nvarchar(max),
   @columnNameSeparator nvarchar(32),
   @previousEndLsn binary(10),
   @startLsn binary(10) = null output,
   @endLsn binary(10) = null output,
   @mask1 varbinary(128) = null output,
   @mask2 varbinary(128) = null output,
   @changesMissed bit = 0 output
) as
begin
/*
   Simple wrapper for GetParams which allows the column names to be passed as a single string with separators rather than a sysnameset.
   A null value for @columnName list will get all columns
*/

   declare @columns [$(CDCX_SCHEMA_NAME)].SysnameSet;

   if (@columnNameList > '''')
   begin
      insert   @columns
      select   ltrim(rtrim(string))
      from     [$(CDCX_SCHEMA_NAME)].Split(@columnNameList, @columnNameSeparator);
   end

   exec [$(CDCX_SCHEMA_NAME)].[@dbAlias?.GetParams]
      @schemaName,
      @tableName,
      @columns,
      @previousEndLsn,
      @startLsn output,
      @endLsn output,
      @mask1 output,
      @mask2 output,
      @changesMissed output;
end';

   set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);

   begin try

      begin tran;
      exec (@ddl);
      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      select failed_setupgetparamsbylist_ddl = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Synonyms](@dbAlias sysname, @cdcDbName sysname = null) as
/*
   Create a set of synonyms pointing to objects in the specified database required by cdcx.
   If @cdcDbName is null then the current database is assumed to be the cdc database
   All cdcx synonyms will use the alias as a qualifier.
   This allows CDCX to work without code changes if the SDLC has evironments where the source db name might be different (eg in dev/uat/prod),
   and also CDCX code to use the same identifier names regardless of whether CDCX is in the CDC database, or a sibling database.
*/
begin

   set xact_abort, nocount on;

   -- synonyms required by core cdcx functionality

   declare @ddl nvarchar(max) = N'
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Schemas];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Tables];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Columns];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.Change_tables];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.Captured_columns];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Indexes];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Index_columns];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.sys.fn_cdc_get_min_lsn];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_increment_lsn];
drop synonym if exists [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_get_max_lsn];
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Schemas] for @db?sys.schemas;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Tables] for @db?sys.tables;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Columns] for @db?sys.columns;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.Change_tables] for @db?cdc.change_tables;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.Captured_columns] for @db?cdc.captured_columns;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Indexes] for @db?sys.indexes;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.Index_columns] for @db?sys.index_columns;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.sys.fn_cdc_get_min_lsn] for @db?sys.fn_cdc_get_min_lsn;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_increment_lsn] for @db?sys.fn_cdc_increment_lsn;
create synonym [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.fn_cdc_get_max_lsn] for @db?sys.fn_cdc_get_max_lsn;
';
   set @ddl = replace(@ddl, '@dbAlias?', @dbAlias);
   set @ddl = replace(@ddl, '@db?', iif(@cdcDbName is null or @cdcDbName = db_name(), '', quotename(@cdcDbName) + '.'));

   begin try

      begin tran;   
      exec (@ddl);  
      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback
      select setup_synonyms_failed_ddl = [$(CDCX_SCHEMA_NAME)].XmlString(@ddl);
      throw 50001, @err, 1;

   end catch

end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Database](@dbAlias sysname, @cdcDbName sysname = null) as
begin
   set xact_abort, nocount on;

   begin try

      begin tran;

      exec [$(CDCX_SCHEMA_NAME)].[Setup.Synonyms] @dbAlias, @cdcDbName;
      exec [$(CDCX_SCHEMA_NAME)].[Setup.ColumnOrdinals] @dbAlias;

      -- Synonyms for change tables need to be recreated if we are changing the database name referred to by the alias

      declare @schemaName sysname, @tableName sysname;

      declare c cursor local fast_forward for 
      select   parsename(base_object_name, 2), parsename(base_object_name, 1)
      from     sys.synonyms   sy
      join     sys.schemas    sc on sc.schema_id = sy.schema_id
      where    sc.name = '$(CDCX_SCHEMA_NAME)'
               and sy.name like '%cdc.changeTables.%';

      open c
      fetch next from c into @schemaName, @tableName;
      while (@@fetch_status = 0)
      begin
         exec [$(CDCX_SCHEMA_NAME)].[Setup.TableSynonyms] @dbAlias, @schemaName, @tableName;
         fetch next from c into @schemaName, @tableName;
      end

      exec [$(CDCX_SCHEMA_NAME)].[Setup.GetParams] @dbAlias;
      exec [$(CDCX_SCHEMA_NAME)].[Setup.GetParamsByList] @dbAlias;

      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;
      throw 50001, @err, 1;

   end catch
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Table](@dbAlias sysname, @schemaName sysname, @tableName sysname) as
begin

   set nocount, xact_abort on;

   -- brief wait in case tables were enabled for cdc just before execution, which might mean cdc metadat is not yet available.

   declare 
      @separator nvarchar(64) = N'########',
      @instance1Expressions nvarchar(max),
      @instance2Expressions nvarchar(max),
      @cutoverLsn char(22),
      @updateMask1DataLength tinyint,
      @updateMask2DataLength tinyint,
      @keyColumnNames nvarchar(max),
      @nonKeyColumnNames nvarchar(max);
      
   exec [$(CDCX_SCHEMA_NAME)].[Setup.GetColumnExpressions]
      @dbAlias, 
      @schemaName, 
      @tableName, 
      @separator,
      @instance1Expressions output,
      @instance2Expressions output,
      @cutoverLsn output,
      @updateMask1DataLength output,
      @updateMask2DataLength output,
      @keyColumnNames output,
      @nonKeyColumnNames output;   

   begin try

      begin tran;

      exec [$(CDCX_SCHEMA_NAME)].[Setup.TableSynonyms] @dbAlias, @schemaName, @tableName;

      exec [$(CDCX_SCHEMA_NAME)].[Setup.Changes]
         @dbAlias, 
         @schemaName, 
         @tableName, 
         @separator,
         @instance1Expressions, 
         @instance2Expressions, 
         @cutoverLsn,
         @updateMask1DataLength,
         @updateMask2DataLength;

      exec [$(CDCX_SCHEMA_NAME)].[Setup.Net]
         @dbAlias,
         @schemaName,
         @tableName,
         @separator,
         @keyColumnNames,
         @nonKeyColumnNames

      commit;
      return 0;

   end try begin catch
      declare @err nvarchar(2048) = object_name(@@procid) + ':' + error_message();
      if (@@trancount > 0) rollback;   
      throw 50001, @err, 1

   end catch      
end
go

create or alter procedure [$(CDCX_SCHEMA_NAME)].[Setup.Tables](@dbAlias sysname) as
begin
   set nocount on;

   create table #tables(schemaName sysname, tableName sysname);

   declare @query nvarchar(max) = N'
insert   #tables
select   s.name, t.name
from     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.cdc.change_tables]  ct
join     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.tables]         t  on t.object_id = ct.source_object_id
join     [$(CDCX_SCHEMA_NAME)].[@dbAlias?.sys.schemas]        s  on s.schema_id = t.schema_id';

   set @query = replace(@query, '@dbAlias?', @dbAlias);
   exec (@query);

   declare @s sysname, @t sysname;

   declare c cursor local fast_forward for select schemaName, tableName from #tables;
   open c
   fetch next from c into @s, @t;
   while (@@fetch_status = 0)
   begin
      exec [$(CDCX_SCHEMA_NAME)].[Setup.Table] @dbAlias, @s, @t;
      fetch next from c into @s, @t;
   end
   close c;
   deallocate c;

end
go

commit;
go

if (@@trancount > 0)
begin
   rollback;
   throw 50001, 'Expected no open transactions', 1;
end
go

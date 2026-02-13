-- CDCX TESTS

-- Name of the database in which CDCX objects were created
:setvar CDCX_DB_NAME 

-- Name of the schema in which CDCX objects were created
:setvar CDCX_SCHEMA_NAME 

-- Name of a CDC enabled database in which objects will be created for cdcx tests (and removed after testing is completed)
:setvar CDCX_TEST_DB_NAME 

-- Determines the volume of data used for performance test. 
-- Set this var to an integer value between 1 and 10 (1 = low volume, 10 = high volume)
:setvar PERFORMANCE_CHECK_VOLUME 1

set nocount on;
set statistics io, time off;
go
set showplan_xml off;
go

use [$(CDCX_TEST_DB_NAME)];
go

if schema_id('cdcx_tests') is not null set noexec on;
go
create schema cdcx_tests authorization dbo;
go
set noexec off;
go

use [$(CDCX_DB_NAME)];
go

exec [$(CDCX_SCHEMA_NAME)].[Setup.Database] 'cdcx_test_db', '$(CDCX_TEST_DB_NAME)';
go

begin try
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_1';
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_2';
end try begin catch
end catch;
go

waitfor delay '00:00:08';
go
/*
drop table if exists [$(CDCX_TEST_DB_NAME)].cdcx_tests.t;
create table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(i int not null primary key clustered, j int, k int);
go

exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_enable_table 'cdcx_tests', 't', 'cdcx_tests_t_1', 1, null, null, 'i, j, k'
go

exec [$(CDCX_SCHEMA_NAME)].[Setup.Table] 'cdcx_test_db', 'cdcx_tests', 't'
go

insert [$(CDCX_TEST_DB_NAME)].cdcx_tests.t (i, j, k) values (1, 1, 1);
insert [$(CDCX_TEST_DB_NAME)].cdcx_tests.t (i, j, k) values (2, 2, 2);

waitfor delay '00:00:08';
go

-- make sure setBit sets the right bit
declare @err nvarchar(2048), @bits varbinary(4) = 0x00000000;
set @bits = [$(CDCX_SCHEMA_NAME)].SetBit(1, @bits); -- 1
set @bits = [$(CDCX_SCHEMA_NAME)].SetBit(2, @bits); -- 2 + 1
set @bits = [$(CDCX_SCHEMA_NAME)].SetBit(9, @bits); -- 256 + 2 + 1
if (259 != cast(@bits as int))
begin
   set @err = concat('test setbit, expected=259, actual=', cast(@bits as int));
   throw 50001, @err, 1;
end
go

-- make sure all rows are returned when no filters are applied ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](0x0, 0xFFFFFFFFFFFFFFFFFFFF, null, null);
if (2 != @actual) print concat('test .Changes with null masks: expected=2, actual=', @actual);
go

-- make sure no rows are returned when the lsn range is invalid ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](0x0, 0x0, null, null);
if (0 != @actual) print concat('test .Changes with null masks and invalid lsn range, expected=0', ' actual=', @actual);
go

-- make sure one row is returned when only the highest lsn is used with no mask ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
declare @maxLsn binary(10) = (select max(__$start_lsn) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct);
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@maxLsn, @maxLsn, null, null) option (recompile);
if (1 != @actual) print concat('test .Changes with null masks high lsn, expected=1', ' actual=', @actual);
go

update [$(CDCX_TEST_DB_NAME)].cdcx_tests.t set j = 11 where i = 1;
update [$(CDCX_TEST_DB_NAME)].cdcx_tests.t set k = 22 where i = 2;
go

waitfor delay '00:00:08';
go

-- make sure correct rows are returned for changes involving only column j (two inserts plus one update) ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'j', ',', 0x0, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @missed output;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@startLsn, @endLsn, @mask1, @mask2);
if (4 != @actual) print concat('test .Changes to column j, expected=4', ' actual=', @actual);
go

-- make sure correct rows are returned for changes involving only column k (two inserts plus one update) ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'k', ',', 0x0, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @missed output;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@startLsn, @endLsn, @mask1, @mask2);
if (4 != @actual) print concat('test .Changes to column k, expected=4', ' actual=', @actual);
go

-- make sure correct rows are returned for changes involving  columns j and k (two inserts two updates) ---------------------------------------------------------------------------------------
declare @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'j,k', ',', 0x0, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @missed output;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@startLsn, @endLsn, @mask1, @mask2);
if (6 != @actual) print concat('test .Changes to column j and k, expected=6', ' actual=', @actual);
go

-- make sure changes missed is zero when no changes are missed ---------------------------------------------------------------------------------------
declare @minlsn binary(10), @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
select @minlsn = min(__$start_lsn) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct;
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'j,k', ',', @minLsn, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @missed output;
if (0 != @missed) print concat('test @missed when no changes missed expected=0', ' actual=', @missed);
go

-- make sure changes missed is one when starting from an lsn that is too low ---------------------------------------------------------------------------------------
declare @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'j,k', ',', 0x0, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @missed output;
if (1 != @missed) print concat('test @missed with an out of bounds lsn, expected=1', ' actual=', @missed);
go   

-- add more columns to source and check functionality for multiple capture instances

alter table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t add l int;
go
exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_enable_table 'cdcx_tests', 't', 'cdcx_tests_t_2', 1, null, null, 'i, j, k, l';
go

exec [$(CDCX_SCHEMA_NAME)].[Setup.Table] 'cdcx_test_db', 'cdcx_tests', 't';
go

insert [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(i, j, k, l) values (3, 3, 3, 3);
update [$(CDCX_TEST_DB_NAME)].cdcx_tests.t set k = 4 where i = 1;
update [$(CDCX_TEST_DB_NAME)].cdcx_tests.t set l = 4 where i = 3;
go

waitfor delay '00:00:08';
go

-- check that all changes from both capture instances are returned when passing null masks
declare @minlsn binary(10), @maxlsn binary(10), @expected int, @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
select @minlsn = min(__$start_lsn), @maxLsn = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_max_lsn(), @expected = count(*) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct;
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@minLsn, @maxLsn, null, null);
if (11 != @actual) print concat('test changes across 2 capture instances with null masks, expected=9', ' actual=', @actual);
go

-- check that column deletions are correctly handled
exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_1';
exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_2';
go

waitfor delay '00:00:08';
go

drop table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t;
create table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(i int primary key, j int, k int);
go

exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_enable_table 'cdcx_tests', 't', 'cdcx_tests_t_1', 1, null, null, 'i, j, k'
go

insert [$(CDCX_TEST_DB_NAME)].cdcx_tests.t (i, j, k) values (1, 1, 1);
go

alter table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t drop column j;
alter table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t add l int;
go

exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_enable_table 'cdcx_tests', 't', 'cdcx_tests_t_2', 1, null, null, 'i, k, l'
go

exec [$(CDCX_SCHEMA_NAME)].[setup.table] 'cdcx_test_db', 'cdcx_tests', 't';
go

insert [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(i, k, l) values (2, 2, 2);
go

waitfor delay '00:00:08';
go

declare @minlsn binary(10), @maxlsn binary(10), @expected int, @actual int, @err nvarchar(1024), @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @missed bit;
select @startLsn = 0x0, @endLsn = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_max_lsn();
select @actual = count(*) from [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.Changes](@startLsn, @endLsn, null, null);
if (2 != @actual) print concat('test changes across 2 capture instances after column deletion with null masks, expected=2', ' actual=', @actual);
go

begin try
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_1';
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_2';
end try begin catch
end catch;
go

waitfor delay '00:00:08';
go

*/
/************************************************************************************************************************************************************
-- performance check: compare cdcx net against microsoft fn_cdc_get_net_changes 
************************************************************************************************************************************************************/

if (not isnull(try_cast('$(PERFORMANCE_CHECK_VOLUME)' as int), -1) between 1 and 10)
begin
   throw 50001, 'PERFORMANCE_CHECK_VOLUME sqlcmd variable must be an integer between 1 and 10 inclusive', 1;
end
go

begin try
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_1';
   exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_2';
end try begin catch
end catch
go

waitfor delay '00:00:08';
go

drop table if exists [$(CDCX_TEST_DB_NAME)].cdcx_tests.t;
create table [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(i int identity(1, 1) primary key clustered, j int, k nchar(256), l nchar(256), m nchar(256));
go

exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_enable_table 'cdcx_tests', 't', 'cdcx_tests_t_1', 1, null, null, 'i, j, k, l, m';
go

exec [$(CDCX_SCHEMA_NAME)].[Setup.Table] 'cdcx_test_db', 'cdcx_tests', 't';
go

declare @inserts int, @updates int, @deletes int;
declare @r int = 1 + ceiling(rand(checksum(newid())) * 5);

insert      [$(CDCX_TEST_DB_NAME)].cdcx_tests.t(j, k, l, m)
select      top 30000            
            1,
            replicate('k', 256), 
            replicate('l', 256),
            replicate('m', 256)
from        sys.all_objects o1
cross join  sys.all_objects o2;
set @inserts = @@rowcount;

update [$(CDCX_TEST_DB_NAME)].cdcx_tests.t set j += 1 where i % @r = 0;
set @updates = @@rowcount;


with r as (select top (@r) * from [$(CDCX_TEST_DB_NAME)].cdcx_tests.t order by newid()) delete from r;
set @deletes = @@rowcount;

waitfor delay '00:00:08';
print concat('batch complete with inserts=', @inserts, ' updates=', @updates, ' deletes=', @deletes);

go $(PERFORMANCE_CHECK_VOLUME)

print 'waiting for cdc to catch up...';
waitfor delay '00:00:10';
go

-- wait for cdc to collect all of the changes
declare @c1 int = (select count(*) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct with (nolock));
declare @c2 int = 0;
while (1 = 1)
begin
   waitfor delay '00:00:05';   
   set @c2 = (select count(*) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct with (nolock));
   if (@c1 = @c2) break;
   else set @c1 = @c2;
end
go

declare @total int, @distinct int;
select @total = count(*), @distinct = count(distinct i) from [$(CDCX_TEST_DB_NAME)].cdc.cdcx_tests_t_1_ct (nolock);
print concat(@total, ' total rows in change table with ', @distinct, ' distinct keys');
go

declare @start binary(10) = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_min_lsn('cdcx_tests_t_1');
declare @end binary(10) = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_max_lsn();
declare @j int = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_column_ordinal('cdcx_tests_t_1', 'j');
declare @k int = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_column_ordinal('cdcx_tests_t_1', 'j');
declare @l int = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_column_ordinal('cdcx_tests_t_1', 'j');
declare @m int = [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_get_column_ordinal('cdcx_tests_t_1', 'j');
print 'ms net changes ------------------------------------';
set statistics time on;
select   count(*)
from     [$(CDCX_TEST_DB_NAME)].cdc.fn_cdc_get_net_changes_cdcx_tests_t_1(@start, @end, 'all with mask')
where    __$operation in (1, 2)
         or [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_is_bit_set(@j, __$update_mask) = 1
         or [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_is_bit_set(@k, __$update_mask) = 1
         or [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_is_bit_set(@l, __$update_mask) = 1
         or [$(CDCX_TEST_DB_NAME)].sys.fn_cdc_is_bit_set(@m, __$update_mask) = 1
option   (recompile);
set statistics time off;
go

declare @start binary(10), @end binary(10), @mask1 varbinary(128);
declare @dt datetime = getdate();
exec [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.GetParamsByList] 'cdcx_tests', 't', 'j,k,l,m', ',', 0x0, @start output, @end output, @mask1 output, null;
print 'cdcx net changes ------------------------------------';
set statistics time on;
select   count(*)
from     [$(CDCX_SCHEMA_NAME)].[cdcx_test_db.cdcx_tests.t.net](@start, @end, @mask1, null) option (recompile);
set statistics time off;
go

waitfor delay '00:00:05';

exec [$(CDCX_TEST_DB_NAME)].sys.sp_cdc_disable_table 'cdcx_tests', 't', 'cdcx_tests_t_1';
go

waitfor delay '00:00:08';
go

use [$(CDCX_TEST_DB_NAME)];
go
drop table cdcx_tests.t;
go
drop schema cdcx_tests;
go

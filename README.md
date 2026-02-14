# Introduction

Extensions to improve performance and developer experience when working SQL Server Change Data Capture.

# What it does

CDCX provides an alternative to using the microsoft `fn_cdc_get_all_changes...` and `fn_cdc_get_net_changes...` functions when reading data from change data capture.

The CDCX implementation is significantly faster than the microsoft implementation (~2 times faster at ~10,000 rows, ~5 times faster at ~100,000 rows, ~10 times faster at ~1,000,000 rows).

It also provides a much more convenient way of specifying the "columns you care about". With the microsoft implementation you must `or` together `fn_cdc_is_bit_set(fn_cdc_get_column_ordinal(...` for every column you care about.\
In the CDCX approach you use a helper procedure to create a bitmask reprsenting columns you care about, and pass that into the `.Changes` or `.Net` function as a parameter. CDCX will also validate the column names you specify (MS does not).

CDCX also provides a convenient an efficient way to get the "before" values of columns. The CDCX `.Net` implementation returns the `__$start_lsn`, `__$operation` and `__$seqval` of the "before" version of the net change. Just join `.Net` to `.Changes` and select the "before" values for columns you're interested in, on the same output row as the "after" values.

Finally, since CDCX is a layer of abstraction over change data capture, it is also able to make changes to CDC relatively transparent.

For example, suppose you needed to alter the columns being tracked by CDC. To do this you must create a second capture instance. In the MS implementation, you now have to change all of the code that was using `get_net_changes` to instead refer to the other capture instance (or both), and once you don't need the old capture instance any more, you need to change all of your code *again*.

In the CDCX implementation, all of this is handled for you. One capture instance? Two capture instances? Different columns on each capture instance? No problem. CDCX hides all of that and automatically combines changes from both capture instances via a single, stable, interface. **No changes are required to code that depends on CDCX!**


# Example (quickstart)

Given...
- A table `MyDatabase.dbo.T(i int primary key clustered, j int, k int, l)` in a database called `MyDb`.
- This table is enabled for change data capture.
- The table has two capture instances. The first capture instance is tracking columns j and k, the second capture instance is tracking columns j, k, and l.
- You want to use CDCX to help get changes from this table for any changes to column j or k.

```sql
-- first run setup_cdcx.sql in a databse of your choosing.
-- For this example we will assume you use the schema name cdcx for the cdcx objects, then...

exec cdcx.[Setup.Database] 'MyAlias', 'MyDb';

exec cdcx.[Setup.Table] 'MyAlias', 'dbo', 'T';

-- In this example I will pass 0x0 as the "previous end LSN".
-- I will also use GetParamsByList and pass in a character separated string representing the columns for which I care about changes.

declare @startLsn binary(10), @endLsn binary(10), @mask1 varbinary(128), @mask2 varbinary(128), @changesMIssed bit;
exec cdcx.[MyAlias.GetParamsByList] 'dbo', 't', 'j, k', ',' 0x0, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @changesMissed output;

-- cdcx_deleted is a bit column; it will be 1 if the net change to the row over the LSN window was deletion.
-- You can also include any valid column in the source table in your select, even if it's not being tracked by CDC.
-- Cdcx will return nulls for columns that are unavailable in CDC.

select  cdcx_deleted, i, j, k 
from    cdcx.[MyAlias.dbo.T.Net](@startLsn, @endLsn, @mask1, @mask2)
option  (recompile); -- recommended

-- If you want all changes instead of net changes, use .Changes instead of .Net.
-- .Changes still returns the cdcx_deleted column.
-- It will be 1 if the operation in the change table was a delate, or a "before update" row (__$operation 1 or 3).

select  cdcx_deleted, i, j, k 
from    cdcx.[MyAlias.dbo.T.C](@startLsn, @endLsn, @mask1, @mask2)
option  (recompile); -- recommended

-- Suppose you want net changes, and you also want the "before" value of column k:

select  n.cdcx_deleted, n.i, n.j, n.k, original_value_of_k = before.k 
from    cdcx.[MyAlias.dbo.T.Net](@startLsn, @endLsn, @mask1, @mask2) n
join    cdcx.[MyAlias.dbo.T.Changes](@startLsn, @endLsn, null, null) before on before.__$start_lsn = n.cdcx_firstStartLsn
                                                                               and before.__$operation = n.cdcx_firstOperation
                                                                               and before.__$seqval = n.cdcx_firstSeqval
option  (recompile); -- recommended

-- You could also do that with cross apply...

select      n.cdcx_deleted, n.i, n.j, n.k, original_value_of_k = before.k 
from        cdcx.[MyAlias.dbo.T.Net](@startLsn, @endLsn, @mask1, @mask2) n
cross apply (
               select k
               from   cdcx.[MyAlias.dbo.T.Changes](n.cdcx_firstStartLsn, n.cdcx_firstStartLsn, null, null)
               where  __$operation = n.cdcx_firstOperation
                      and __$seqval = n.cdcx_firstSeqval
            ) before
option      (recompile);             
```

# Installation

The file `cdcx_setup.sql` creates a "library" of objects in a database and schema of your choosing.

Run `cdcx_setup.sql` and provide values for the `CDCX_DB_NAME` and `CDCX_SCHEMA_NAME` sqlcmd variables (at the top of the file).\
You can run this in the CDC enabled database, or on another database on the same SQL Server instance.

A default value of "CDCX" is already provided for `CDCX_SCHEMA_NAME`, but you can change this.

After `cdcx_setup.sql` has been executed, you can use the "Setup" procedures it created to start creating CDCX helpers for change data capture tables in this, or other, databases.

## Setting up CDCX to Point to a CDC-Enabled Database

### `exec cdcx.[Setup.Database] @dbAlias, @cdcDbName;`

`@dbAlias (sysname)` :
<ul>
A name that will be used throughout CDCX objects when referring to the database containing CDC objects you want CDCX to help you with. This alias ensures consistent cdcx object names even if your CDC enabled database has different names in different environments.
</ul>

`@cdcDbName (sysname = null)` :
<ul>
The name of the cdc-enabled database to which you want to refer. If the cdc-enabled database is the current database you do not have to pass this parameter.
</ul>

### Notes 

- You can run `cdcx.[Setup.Database]` multiple times with the same arguments with no ill-effects (it is idempotent).
- You can also run `cdcx.[Setup.Database]` multiple times for different databases with different aliases if you want to. 

For example, if you installed cdcx into "databaseA", and you want to use cdcx to help you with objects in both "databaseA" and "databaseB", you can do this:

```sql
use databaseA;
exec cdcx.[Setup.Database] 'this'; --  Will create syonyms required by cdcx for objects which exist in databaseA
exec cdcx.[Setup.Database] 'this'; --  running it multiple times with the same arguments is fine, it's idempotent.
exec cdcx.[Setup.Database] 'B', 'databaseB'; -- will create synonyms required by cdcx for objects which exist in databaseB
```
---

## Setting up CDCX for a Particular Source Table

Once you have run `cdcx.[Setup.Database]`, you can then run `cdcx.[Setup.Table]` for any table in that database which is being tracked by CDC:

### `exec cdcx.[Setup.Table] @dbAlias, @schemaName, @tableName;`

`@dbAlias (sysname)` :
<ul>
The alias used when you executed [Setup.Database]
</ul>

`@schemaName (sysname)` :
<ul>
The schema name of the cdc-tracked table in the aliased database.  
</ul>

`@tableName (sysname)` :
<ul>
The table name of the cdc-tracked table in the aliased database.  
</ul>

`[Setup.Table]` will create new functions in your chosen cdcx schema:
- `cdcx.[<your database alias>.<source schema name>.<source table name>.Changes](@startLsn, @endLsn, @mask1, @mask2)`
- `cdcx.[<your database alias>.<source schema name>.<source table name>.Net](@startLsn, @endLsn, @mask1, @mask2)`

For both of these functions, the parameters are as follows:

`@startLsn (binary(10))` :
<ul>
The first LSN, inclusive, from which changes in the CDC change table (or tables) will be returned.
</ul>

`@endLsn (binary(10))` :
<ul>
The last LSN, inclusive, from which changes in the CDC change table (or tables) will be returned.
</ul>

`@mask1 (varbinary(128)) = null` :
<ul>
A bitmask which specifiefs which columns you "care about" in the first capture table. Changes will only be returned for changes that involve this/these columns.
Passing null for this parameter will get all changes (not filtered to changes to specific columns).
</ul>

`@mask2 (varbinary(128)) = null` :
<ul>
A bitmask which specifiefs which columns you "care about" in the second capture table (if a second capture table exists). 
Changes will only be returned for changes that involve this/these columns.
Passing null for this parameter will get all changes (not filtered to changes to specific columns).
This parameter has no effect if there is only one capture instance for the source table.
</ul>

### Notes 

- You can run `cdcx.[Setup.Table]` multiple times with the same arguments with no ill-effects (it is idempotent).
- If you change the underlying table schema or the underlying CDC schema (eg, you create a second capture instance with more tracked columns, or you delete a column fro the source table), just re-run `ccx.[Setup.Table]`.


**You might be thinking that you still have to worry about validating your LSN range. And how do you get the value for `@mask1`? And how do you know whether you need a value for `@mask2`, and how do you get that?**

**Enter `GetParams`...**

## `exec cdcx.[<your database alias>.GetParams] @schemaName, @tableName, @columns, @previousEndLsn, @startLsn output, @endLsn output, @mask1 output, @mask2 output, @changesMissed output;`

`@schemaName (sysname)` :
<ul>
The schema name of the cdc enabled source table
</ul>

`@tableName (sysname)` :
<ul>
The table name of the cdc enabled source table
</ul>

`@columns (cdcx.sysnameset)` :
<ul>
A table valued parameter containing the column names you care about. (Note: you can also call cdcx.[<your db alias>.GetParamsByList] and pass a character separated string instead).
</ul>

`@previousEndLsn (binary(10))` :
<ul>
Where are you up to? This should be the same as the value of a previous @endLsn. CDCX will bounds-check this for you.
</ul>

`@startLsn (binary(10) output)` :
<ul>
This output paramter provides a bounds-checked start LSN. Use as the input @startLsn value for your next call to a cdcx .Changes or .Net function.
</ul>

`@endLsn (binary(10) output)` :
<ul>
This output paramter provides a new high watermark. Use as the input @endLsn value for your next call to a cdcx .Changes or .Net function.
</ul>

`@mask1 (varbinary(128) output)` :
<ul>
A bitmask representing the columns you care about. Use as the input @mask1 value for your next call to cdcx .Changes or .Net. Passing null for this parameter will get all changes (not filtered to changes to specific columns).
</ul>

`@mask2 (varbinary(128) output)` :
<ul>
A bitmask representing the columns you care about. Use as the input @mask2 value for your next call to cdcx .Changes or .Net.
(If there is only one capture instance this parameter isn't needed. But you don't need to know that. Just call GetParams and hand off the output values to .Changes or .Net!)
</ul>

`@mchangesMissed (bit) output)` :
<ul>
A bit value which indicates whether or not changes were missed.
The GetParams procedure automatically bounds checks your LSN's for you.
If your input @previousEndLsn represents a point that is no longer available in the underlying capture tables, it means you have missed some change data (it has aged-out of the capture tables).
This output parameter provides you with a notification that this has happened, so you can take corrective action if needed.
</ul>


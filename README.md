# Introduction

Extensions to improve performance and developer experience when working SQL Server Change Data Capture.

# Installation

cdcx_setup.sql creates a "library" of objects in a database and schema of your choosing.\
Run `cdcx_setup.sql` and provide values for the `CDCX_DB_NAME` and `CDCX_SCHEMA_NAME` sqlcmd variables (at the top of the file).\
A default value of "CDCX" is already provided for `CDCX_SCHEMA_NAME`, but you can change this.

After `cdcx_setup.sql` has been executed, you can use the "Setup" procedures it created to start creating CDCX helpers for change data capture tables in this, or other, databases.

---

First `exec cdcx.[Setup.Database] @dbAlias, @cdcDbName`

`@dbAlias (sysname)` :
<ul>
A name that will be used throughout CDCX objects when referring to the database containing CDC objects you want CDCX to help you with.
  
This alias ensures consistent cdcx object names even if your CDC enabled database has different names in different environments, or you want to track the same database in which you ran cdcx_setup.sql.
</ul>

`@cdcDbName (sysname = null)` :
<ul>
The name of the cdc-enabled database to which you want to refer.
  
If the cdc-enabled database is the same database into which you ran cdcx_setup.sql, you do not have to pass this parameter.
</ul>

## Note 

You can run `cdcx.[Setup.Database]` multiple times for different databases with different aliases if you want to. 

For example, if you installed cdcx into "databaseA", and you want to use cdcx to help you with objects in both "databaseA" and "databaseB", you can do this:

```sql
use databaseA;
exec cdcx.[Setup.Database] 'this'; -- this will create necessary references to cdc objects in databaseA;
exec cdcx.[Setup.Database] 'B', 'databaseB';
```

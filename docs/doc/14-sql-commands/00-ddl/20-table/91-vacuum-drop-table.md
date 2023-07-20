---
title: VACUUM DROP TABLE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.10"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='VACUUM DROP TABLE'/>

The VACUUM DROP TABLE command helps save storage space by permanently removing data files of dropped tables, freeing up storage space, and enabling you to manage the process efficiently. It offers options to target specific databases, set retention times, and preview the data files that will be vacuumed. To list the dropped tables of a database, use [SHOW DROP TABLES](../../40-show/show-drop-tables.md).

See also: [VACUUM TABLE](91-vacuum-table.md)

### Syntax and Examples

```sql
VACUUM DROP TABLE [FROM <database_name>] [RETAIN n HOURS] [DRY RUN]
```
- **FROM <database_name>**: This option restricts the search for dropped tables to a specific database. If not specified, the command will scan all databases, including those that have been dropped.

    ```sql
    -- Remove dropped tables from the "default" database
    VACUUM DROP TABLE FROM default;

    -- Remove dropped tables from all databases
    VACUUM DROP TABLE;
    ```

- **RETAIN n HOURS**: This option determines the retention status of data files for dropped tables, removing only those that were created more than *n* hours ago. In the absence of this parameter, the command defaults to the `retention_period` setting (usually set to 12 hours), leading to the removal of data files older than 12 hours during the vacuuming process.

    ```sql
    -- Remove data files older than 24 hours for dropped tables
    VACUUM DROP TABLE RETAIN 24 HOURS;
    ```

- **DRY RUN**: When this option is specified, data files will not be removed, instead, a list of up to 100 candidate files will be returned that would have been removed if the option was not used. This is useful when you want to preview the potential impact of the VACUUM DROP TABLE command before actually removing any data files. For example:

    ```sql
    -- Preview data files to be removed for dropped tables
    VACUUM DROP TABLE DRY RUN;

    -- Preview data files to be removed for dropped tables in the "default" database
    VACUUM DROP TABLE FROM default DRY RUN;

    -- Preview data files to be removed for dropped tables older than 24 hours
    VACUUM DROP TABLE RETAIN 24 HOURS DRY RUN;
    ```
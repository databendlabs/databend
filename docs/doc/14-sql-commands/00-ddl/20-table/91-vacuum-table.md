---
title: VACUUM TABLE
---
import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='VACUUM TABLE'/>

The VACUUM TABLE command helps to optimize the system performance by freeing up storage space through the permanent removal of historical data files from a table. This includes:

- Snapshots, as well as their relevant segments and blocks. 

- Orphan files. Orphan files in Databend refer to snapshots, segments, and blocks that are no longer associated with the table. Orphan files might be generated from various operations and errors, such as during data backups and restores, and can take up valuable disk space and degrade the system performance over time.


### Syntax and Examples

```sql
VACUUM TABLE <table_name> [RETAIN n HOURS] [DRY RUN]
```

- **RETAIN n HOURS**: This option determines which historical data files are removed or retained. When this option is specified, only data files that were created more than *n* hours ago will be removed. If this option is not specified, the default `retention_period` setting of 12 hours will be applied instead. This means that any historical data files that are older than 12 hours will be removed.

- **DRY RUN**: When this option is specified, candidate orphan files will not be removed, instead, a list of up to 1,000 candidate files will be returned that would have been removed if the option was not used. This is useful when you want to preview the potential impact of the VACUUM TABLE command on the table before actually removing any data files. For example:

    ```sql
    VACUUM TABLE t RETAIN 0 HOURS DRY RUN;

    +-----------------------------------------------------+
    | Files                                               |
    +-----------------------------------------------------+
    | 1/8/_sg/932addea38c64393b82cb4b8fb7a2177_v3.bincode |
    | 1/8/_b/b68cbe5fe015474d85a92d5f7d1b5d99_v2.parquet  |
    +-----------------------------------------------------+
    ```

### VACUUM TABLE vs. OPTIMIZE TABLE

Databend provides two commands for removing historical data files from a table: VACUUM TABLE and [OPTIMIZE TABLE](60-optimize-table.md) (with the PURGE option). While both commands permanently remove data files, only VACUUM TABLE can remove orphan files and free up additional storage space.

Both VACUUM TABLE and OPTIMIZE TABLE allow you to specify a period to determine which historical data files to remove. However, OPTIMIZE TABLE  requires you to obtain the snapshot ID or timestamp from a query beforehand, whereas VACUUM TABLE allows you to specify the number of hours to retain the data files directly.

In addition, VACUUM TABLE offers the DRY RUN option, which allows you to preview the data files to be removed before applying the command. This provides a safe removal experience and helps you avoid unintended data loss.
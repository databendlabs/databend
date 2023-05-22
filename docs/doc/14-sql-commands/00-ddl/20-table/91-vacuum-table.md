---
title: VACUUM TABLE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='VACUUM TABLE'/>

The VACUUM TABLE command helps to optimize the system performance by freeing up storage space through the permanent removal of historical data files from a table. This includes:

- Snapshots associated with the table, as well as their relevant segments and blocks.

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

Databend provides two commands for removing historical data files from a table: VACUUM TABLE and [OPTIMIZE TABLE](60-optimize-table.md) (with the PURGE option). Although both commands are capable of permanently deleting data files, they differ in how they handle orphan files: OPTIMIZE TABLE is able to remove orphan snapshots, as well as the corresponding segments and blocks. However, there is a possibility of orphan segments and blocks existing without any associated snapshots. In such a scenario, only VACUUM TABLE can help clean them up.

Both VACUUM TABLE and OPTIMIZE TABLE allow you to specify a period to determine which historical data files to remove. However, OPTIMIZE TABLE requires you to obtain the snapshot ID or timestamp from a query beforehand, whereas VACUUM TABLE allows you to specify the number of hours to retain the data files directly.

VACUUM TABLE provides enhanced control over your historical data files both before and after their removal:

- VACUUM TABLE includes the DRY RUN option, which allows you to preview the data files to be removed before applying the command. This provides a safe removal experience and helps you avoid unintended data loss. 
- VACUUM TABLE comes with a fail-safe service that provides a 7-day window for potential data recovery. This means that if you remove data files, there is a possibility to recover them within 7 days. If you need this service, please contact [Databend Team](https://www.databend.com/contact-us/) for assistance.

|                                                  	| VACUUM TABLE 	| OPTIMIZE TABLE 	|
|--------------------------------------------------	|--------------	|----------------	|
| Associated snapshots (incl. segments and blocks) 	| Yes          	| Yes            	|
| Orphan snapshots (incl. segments and blocks)     	| Yes          	| Yes            	|
| Orphan segments and blocks only                  	| Yes          	| No             	|
| DRY RUN                                         	| Yes          	| No             	|
| Fail-safe                                         | Yes          	| No             	|

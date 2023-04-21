---
title: system.clusters
---

Contains information about cluster nodes.

:::note 
You can disable access to `clusters` table using the configuration option `disable_system_table_load`.

For instance, users of DatabendCloud will not be able to see this table. 
:::

```sql
SELECT * FROM system.clusters;
+------------------------+---------+------+
| name                   | host    | port |
+------------------------+---------+------+
| 2KTgGnTDuKHw3wu9CCVIf6 | 0.0.0.0 | 9093 |
| bZTEWpQGLwRgcRyHre1xL3 | 0.0.0.0 | 9092 |
| plhQlHvVfT0p1T5QdnvhC4 | 0.0.0.0 | 9091 |
+------------------------+---------+------+
```
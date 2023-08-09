---
title: MASKING POLICY
---

Databend supports the functionality of Masking Policy. A Masking Policy defines rules for displaying sensitive data selectively. It is essential for protecting confidential information while still allowing authorized users to access certain parts of the data. Masking Policy provides a controlled way to reveal or hide sensitive details, ensuring data security and compliance with privacy regulations.

After you create a masking policy, you need to associate it with a column containing sensitive data. This linkage enables Databend to systematically apply the defined masking rules to the specific dataset column. By establishing this connection, the masking policy becomes tailored to the exact context where data privacy is paramount. This column-level association ensures that sensitive information remains protected while still permitting authorized users to interact with the data they require. To associate a masking policy with a column, use the [ALTER TABLE COLUMN](../20-table/90-alter-table-column.md) command.
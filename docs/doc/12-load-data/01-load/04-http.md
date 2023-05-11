---
title: Loading from Remote File
---

To load data from remote files into Databend, the [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command can be used. This command allows you to copy data from a variety of sources, including remote files, into Databend with ease. With COPY INTO, you can specify the source file location, file format, and other relevant parameters to tailor the import process to your needs. Please note that the files must be in a format supported by Databend, otherwise the data cannot be imported. For more information on the file formats supported by Databend, see [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md).

## Tutorial - Load from a Remote File

### Before You Begin

Download and save the sample file [books.csv](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv) to a local folder. The file contains two records:

```text
Transaction Processing,Jim Gray,1992
Readings in Database Systems,Michael Stonebraker,2004
```

### Step 1. Create Database and Table

```sql
CREATE DATABASE book_db;
USE book_db;

CREATE TABLE books
(
    title VARCHAR,
    author VARCHAR,
    date VARCHAR
);
```

### Step 2. Load Data into Table

```sql
COPY INTO books FROM 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv'
    FILE_FORMAT = (type = CSV field_delimiter = ','  record_delimiter = '\n' skip_header = 0);
```

:::tip

The command can also load data from multiple files that are sequentially named. See [COPY INTO `<table>`](../../14-sql-commands/10-dml/dml-copy-into-table.md) for details.

:::

### Step 3. Verify Loaded Data

```sql
SELECT * FROM books;
+------------------------------+----------------------+-------+
| title                        | author               | date  |
+------------------------------+----------------------+-------+
| Transaction Processing       |  Jim Gray            |  1992 |
| Readings in Database Systems |  Michael Stonebraker |  2004 |
+------------------------------+----------------------+-------+
```
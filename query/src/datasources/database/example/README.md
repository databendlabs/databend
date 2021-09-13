# How to add a new datasource?

Datasource is the source of all data for queries in Datafuse, and also provides the database, table, view schema.

In Datafuse, datasource is pluggable, it's easy to add a new datasource.

## Datasource

By default, Datafuse has three built-in datasource:
* LOCAL - which is in `local` directory, maintains all the local database and table schema, it is used mainly for testing.
* SYSTEM - which is in `system` directory, it maintains all the system database and table schema.
* REMOTE - which is in `remote` directory, and maintains all the remote database and tables.

## Add new datasource

In this example, we add a datasource called `EXAMPLE`, it's something like a black hole, and we register it to the Catalog, then run some queries:
```sql
mysql> CREATE DATABASE example_db ENGINE=EXAMPLE;
Query OK, 0 rows affected (0.01 sec)

mysql> use example_db;

Database changed
mysql> CREATE TABLE t1(a int);
Query OK, 0 rows affected (0.00 sec)

mysql> show tables;
+------+
| name |
+------+
| t1   |
+------+
1 row in set (0.01 sec)

mysql> DESC t1;
+-------+-------+------+
| Field | Type  | Null |
+-------+-------+------+
| a     | Int32 | NO   |
+-------+-------+------+
1 row in set (0.00 sec)
```

Next, we will introduce 4 components:
* meta backend
* databases
* database
* table

```
         ┌───────────────────────────────────────────────► │
         │                                                 │
         │                  ┌────────────────────────────► │
         │                  │                              │
    ┌────┴─────┐        ┌───┴────┐         ┌──────────┐    │   ┌──────────┐
    │          │        │        ├────────►│  table   ├───►│   │          │
    │          │        │        │         └──────────┘    │   │          │
    │          │        │database│                         │   │          │
    │          ├───────►│        │                         │   │          │
    │          │        │        │         ┌──────────┐    │   │          │
    │          │        │        ├────────►│  table   ├───►│   │          │
    │ databases│        └────────┘         └──────────┘    │   │  meta    │
    │          │        ┌────────┐         ┌──────────┐    │   │  backend │
    │          │        │        ├────────►│  table   ├─-─►│   │          │
    │          │        │database│         └──────────┘    │   │          │
    │          ├───────►│        │                         │   │          │
    │          │        │        │                         │   │          │
    │          │        │        │         ┌──────────┐    │   │          │
    │          │        │        ├────────►│  table   ├─-─►│   │          │
    └──────────┘        └────────┘         └──────────┘    │   └──────────┘
                                                           │
```

### Meta Backend

The meta backend tells where the database and table schema information is from, we need implement the `MetaBackend` trait:
```rust
pub trait MetaBackend: Sync + Send {
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>>;
    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableMeta>>>;
    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>>;
    fn exists_database(&self, _db_name: &str) -> Result<bool>;
    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}
```
See [example_meta_backend.rs](example_meta_backend.rs)

### Databases

`example_databases.rs` is a wrapper for maintaining all the database, we can do some operations against the database, it needs implement `DatabaseEngine` trait:
```rust
/// Database engine type, for maintaining databases, like create/drop or others lookup.
pub trait DatabaseEngine: Send + Sync {
    // Engine name of the database.
    fn engine_name(&self) -> &str;
    // Get the database by db_name.
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    // Check the database is exists or not.
    fn exists_database(&self, db_name: &str) -> Result<bool>;
    // Get all the databases of this backend.
    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>>;

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}
```
See [example_databases.rs](example_databases.rs)


### Database

Database provides a number of table-specific operations, like:
```rust
pub trait Database: Sync + Send {
    /// Database name.
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn is_local(&self) -> bool;

    /// Get the table by name.
    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>>;
    /// Check the table exists or not.
    fn exists_table(&self, table_name: &str) -> Result<bool>;

    /// Get table by meta id
    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>>;

    /// Get all tables.
    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>>;

    /// Get database table functions.
    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>>;

    /// DDL
    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
}
```

See [example_database.rs](example_database.rs)

### Table

Table is the data source, it provides a number of data operations, like:
```rust
pub trait Table: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;
    // Is Local or Remote.
    fn is_local(&self) -> bool;
    // Get the read source plan.
    fn read_plan(
        &self,
        ctx: DatafuseQueryContextRef,
        scan: &ScanPlan,
        partitions: usize,
    ) -> Result<ReadDataSourcePlan>;
    // Read block data from the underling.
    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _ctx: DatafuseQueryContextRef,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _ctx: DatafuseQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}
```
See [example_table.rs](example_table.rs)

### Register

Register the `EXAMPLE` datasource to Catalog in sessions.rs:
```rust
let catalog = Arc::new(DatabaseCatalog::try_create_with_config(conf.clone())?);
// Register local/system and remote database engine.
catalog.register_db_engine("LOCAL", Arc::new(LocalDatabases::create(conf.clone())))?;
catalog.register_db_engine("SYSTEM", Arc::new(SystemDatabases::create(conf.clone())))?;
catalog.register_db_engine("REMOTE", Arc::new(RemoteDatabases::create(conf.clone())))?;
// Register the example for demo.
catalog.register_db_engine("EXAMPLE", Arc::new(ExampleDatabases::create(conf.clone())))?;

```
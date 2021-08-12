// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//
use std::collections::HashMap;
use std::time::Duration;

use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
use common_datavalues::prelude::Arc;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::meta_api_impl::CreateDatabaseActionResult;
use common_flights::meta_api_impl::CreateTableActionResult;
use common_flights::meta_api_impl::DatabaseMetaReply;
use common_flights::meta_api_impl::DropDatabaseActionResult;
use common_flights::meta_api_impl::DropTableActionResult;
use common_flights::meta_api_impl::GetDatabaseActionResult;
use common_flights::meta_api_impl::GetTableActionResult;
use common_flights::storage_api_impl::AppendResult;
use common_flights::storage_api_impl::BlockStream;
use common_flights::storage_api_impl::ReadAction;
use common_flights::storage_api_impl::ReadPlanResult;
use common_flights::storage_api_impl::TruncateTableResult;
use common_flights::StorageApi;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ScanPlan;
use common_planners::TableEngineType;
use common_runtime::tokio;
use common_store_api::DatabaseMetaSnapshot;
use common_store_api::MetaApi;
use common_streams::SendableDataBlockStream;
use TableEngineType::JSONEachRow;

use crate::catalogs::impls::remote_meta_store_client::RemoteMetaStoreClient;
use crate::catalogs::meta_store_client::DBMetaStoreClient;
use crate::datasources::remote::GetStoreApiClient;
use crate::datasources::remote::StoreApis;

struct FakeStoreApisProvider {
    apis: FakeStoreApis,
}

impl FakeStoreApisProvider {
    fn new(apis: FakeStoreApis) -> Self {
        Self { apis }
    }
}

#[async_trait::async_trait]
impl GetStoreApiClient<FakeStoreApis> for FakeStoreApisProvider {
    async fn try_get_store_apis(&self) -> Result<FakeStoreApis> {
        Ok(self.apis.clone())
    }
}

#[derive(Clone)]
struct TableInfo {
    id: u64,
    ver: u64,
    name: String,
    schema: DataSchemaRef,
}

#[derive(Clone)]
struct DbInfo {
    id: u64,
    tables: HashMap<String, TableInfo>,
}
#[derive(Clone)]
struct FakeStoreApis {
    dbs: HashMap<String, DbInfo>,
    inject_timeout: Option<Duration>,
    inject_inconsistent_meta_state: bool,
    inject_invalid_schema: bool,
    tbl_id_seq: u64,
}
impl FakeStoreApis {
    pub fn new() -> Self {
        FakeStoreApis {
            dbs: HashMap::new(),
            inject_timeout: None,
            inject_inconsistent_meta_state: false,
            inject_invalid_schema: false,
            tbl_id_seq: 0,
        }
    }

    pub fn next_tbl_id(&mut self) -> u64 {
        self.tbl_id_seq = self.tbl_id_seq + 1;
        self.tbl_id_seq
    }

    pub fn insert_db(&mut self, id: u64, name: impl Into<String>) {
        self.dbs.insert(name.into(), DbInfo {
            id,
            tables: Default::default(),
        });
    }

    pub fn insert_tbl(&mut self, db_name: &str, tbl: TableInfo) {
        let db = self.dbs.get_mut(db_name).unwrap();
        db.tables.insert(tbl.name.clone(), tbl);
    }

    pub fn inject_timeout(&mut self, duration: Duration) {
        self.inject_timeout = Some(duration);
    }
}
impl StoreApis for FakeStoreApis {}

#[async_trait::async_trait]
impl StorageApi for FakeStoreApis {
    async fn read_plan(
        &mut self,
        _db_name: String,
        _tbl_name: String,
        _scan_plan: &ScanPlan,
    ) -> Result<ReadPlanResult> {
        todo!()
    }

    async fn read_partition(
        &mut self,
        _schema: DataSchemaRef,
        _read_action: &ReadAction,
    ) -> Result<SendableDataBlockStream> {
        todo!()
    }

    async fn append_data(
        &mut self,
        _db_name: String,
        _tbl_name: String,
        _scheme_ref: DataSchemaRef,
        _block_stream: BlockStream,
    ) -> Result<AppendResult> {
        todo!()
    }

    async fn truncate(&mut self, _db: String, _table: String) -> Result<TruncateTableResult> {
        todo!()
    }
}

#[async_trait::async_trait]
impl MetaApi for FakeStoreApis {
    async fn create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> Result<CreateDatabaseActionResult> {
        let r = self.dbs.get(&plan.db);
        match r {
            None => Ok(CreateDatabaseActionResult { database_id: 0 }), // ignore db_id
            Some(_db) if plan.if_not_exists => Ok(CreateDatabaseActionResult { database_id: 0 }),
            Some(_) => Err(ErrorCode::DatabaseAlreadyExists("")),
        }
    }

    async fn get_database(&mut self, db_name: &str) -> Result<GetDatabaseActionResult> {
        // timeout fault only enabled for get_database operation
        if let Some(_) = self.inject_timeout {
            return Err(ErrorCode::Timeout(""));
        }
        self.dbs
            .get(db_name)
            .map(|db| GetDatabaseActionResult {
                database_id: db.id,
                db: db_name.to_owned(),
            })
            .ok_or_else(|| ErrorCode::UnknownDatabase(format!("database {} not found", db_name)))
    }

    async fn drop_database(&mut self, plan: DropDatabasePlan) -> Result<DropDatabaseActionResult> {
        if self.dbs.contains_key(&plan.db) {
            Ok(DropDatabaseActionResult {})
        } else {
            if plan.if_exists {
                Ok(DropDatabaseActionResult {})
            } else {
                Err(ErrorCode::UnknownDatabase(""))
            }
        }
    }

    async fn create_table(&mut self, plan: CreateTablePlan) -> Result<CreateTableActionResult> {
        let r = self.dbs.get(&plan.db);
        match r {
            None => Err(ErrorCode::UnknownDatabase("")),
            Some(db) => match db.tables.get(&plan.table) {
                Some(t) if plan.if_not_exists => Ok(CreateTableActionResult { table_id: t.id }),
                Some(_t) => Err(ErrorCode::TableAlreadyExists("")),
                None => Ok(CreateTableActionResult {
                    table_id: self.next_tbl_id(),
                }),
            },
        }
    }

    async fn drop_table(&mut self, plan: DropTablePlan) -> Result<DropTableActionResult> {
        let r = self.dbs.get(&plan.db);
        match r {
            None => Err(ErrorCode::UnknownDatabase("")),
            Some(db) => match db.tables.get(&plan.table) {
                None if plan.if_exists => Ok(DropTableActionResult {}),
                None => Err(ErrorCode::UnknownTable("")),
                Some(_t) => Ok(DropTableActionResult {}),
            },
        }
    }

    async fn get_table(&mut self, db_name: String, table: String) -> Result<GetTableActionResult> {
        let r = self.dbs.get(&db_name);
        match r {
            None => Err(ErrorCode::UnknownDatabase("")),
            Some(db) => match db.tables.get(&table) {
                None => Err(ErrorCode::UnknownTable("")),
                Some(t) => Ok(GetTableActionResult {
                    table_id: t.id,
                    db: db_name,
                    name: "".to_string(),
                    schema: t.schema.clone(),
                }),
            },
        }
    }

    async fn get_table_ext(
        &mut self,
        table_id: MetaId,
        db_ver: Option<MetaVersion>,
    ) -> Result<GetTableActionResult> {
        for (db_name, db_info) in &self.dbs {
            for (table_name, table_info) in &db_info.tables {
                if table_info.id == table_id && table_info.ver == db_ver.unwrap_or(0) {
                    return Ok(GetTableActionResult {
                        table_id: table_info.id,
                        db: db_name.to_owned(),
                        name: table_name.to_string(),
                        schema: table_info.schema.clone(),
                    });
                }
            }
        }

        Err(ErrorCode::UnknownTable(""))
    }

    async fn get_database_meta(&mut self, _current_ver: Option<u64>) -> Result<DatabaseMetaReply> {
        use common_arrow::arrow::io::ipc::write::common::IpcWriteOptions;
        let tbl_metas = if self.inject_inconsistent_meta_state {
            vec![]
        } else {
            let mut res = vec![];
            for (_db_name, db) in &self.dbs {
                for (_tbl_name, tbl) in &db.tables {
                    let schema_bytes = if self.inject_invalid_schema {
                        vec![]
                    } else {
                        let options = IpcWriteOptions::default();
                        let flight_data =
                            flight_data_from_arrow_schema(&tbl.schema.to_arrow(), &options);
                        flight_data.data_header
                    };
                    res.push((tbl.id, common_metatypes::Table {
                        table_id: tbl.id,
                        schema: schema_bytes,
                        parts: Default::default(),
                    }))
                }
            }
            res
        };

        let reply = Some(DatabaseMetaSnapshot {
            meta_ver: 0,
            db_metas: self
                .dbs
                .iter()
                .map(|(n, v)| {
                    (n.to_string(), common_metatypes::Database {
                        database_id: v.id,
                        tables: v
                            .tables
                            .iter()
                            .map(|(k, v)| (k.clone(), v.id))
                            .collect::<HashMap<_, _>>(),
                    })
                })
                .collect(),
            tbl_metas: tbl_metas,
        });
        Ok(reply)
    }
}

#[test]
fn test_get_database() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::with_timeout_setting(provider, None);
    let res = store_client.get_database("test_db");

    // db not exists
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!(e.code(), ErrorCode::UnknownDatabase("").code());
    }

    // db does exists
    let res = store_client.get_database("test")?;
    assert_eq!(res.name(), "test");

    Ok(())
}

#[test]
fn test_get_databases() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    // get databases
    let res = store_client.get_databases()?;
    assert_eq!(res, vec!["test"]);

    Ok(())
}

#[test]
fn test_get_table() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::with_timeout_setting(provider, None);

    // table exist
    let res = store_client.get_table("test", "t1");
    assert!(res.is_ok());
    let res = res?;
    assert_eq!(res.meta_id(), 0);
    // table version not implemented in persistent store yet, we fake this shortcoming as well
    assert_eq!(res.meta_ver(), None);

    // table does not exist
    let res = store_client.get_table("test", "t2");
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!(e.code(), ErrorCode::UnknownTable("").code());
    }

    // db does not exist
    let res = store_client.get_table("test1", "t2");
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!(e.code(), ErrorCode::UnknownDatabase("").code());
    }

    Ok(())
}

#[test]
fn test_get_all_tables() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });
    fake_apis.inject_inconsistent_meta_state = true;

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::with_timeout_setting(provider, None);

    // illegal meta state
    let res = store_client.get_all_tables();
    assert!(res.is_err());
    if let Err(err) = res {
        assert_eq!(err.code(), ErrorCode::IllegalMetaState("").code());
    }

    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    // normal case
    let res = store_client.get_all_tables()?;
    assert_eq!(res.len(), 1);
    Ok(())
}

#[test]
fn test_get_table_by_id() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    let res = store_client.get_table_by_id("test", 0, None);
    assert!(res.is_ok());
    Ok(())
}

#[test]
fn test_get_db_tables() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    let res = store_client.get_databases()?;
    assert_eq!(1, res.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_table() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));

    // we do not need to disable timeout here to avoid flaky test, since `create` is NOT implemented
    // by RemoteMetaStoreClient::do_block , which is flaky, partially due to that while constructing
    // the ErrorCode, a new backtrace stack is populated, and the async call stack may be deep,
    // takes seconds(flaky) to be built, thus if rpc timeout parameter is not properly tuned,  an
    // ErrorCode::Timeout might be returned, instead of the ErrorCode we expects.
    let store_client = RemoteMetaStoreClient::create(provider);

    // db not exist
    let res = store_client
        .create_table(CreateTablePlan {
            if_not_exists: false,
            db: "t".to_string(),
            table: "t1".to_string(),
            schema: Arc::new(DataSchema::empty()),
            engine: JSONEachRow,
            options: Default::default(),
        })
        .await;
    assert!(res.is_err());
    if let Err(err) = res {
        assert_eq!(err.code(), ErrorCode::UnknownDatabase("").code());
    }

    // normal
    let res = store_client
        .create_table(CreateTablePlan {
            if_not_exists: false,
            db: "test".to_string(),
            table: "t2".to_string(),
            schema: Arc::new(DataSchema::empty()),
            engine: JSONEachRow,
            options: Default::default(),
        })
        .await;
    assert!(res.is_ok());

    // normal
    let res = store_client
        .create_table(CreateTablePlan {
            if_not_exists: true,
            db: "test".to_string(),
            table: "t1".to_string(),
            schema: Arc::new(DataSchema::empty()),
            engine: JSONEachRow,
            options: Default::default(),
        })
        .await;
    assert!(res.is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_table() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    // db not exist
    let res = store_client
        .drop_table(DropTablePlan {
            if_exists: false,
            db: "t".to_string(),
            table: "t1".to_string(),
        })
        .await;
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!(e.code(), ErrorCode::UnknownDatabase("").code());
    }

    // normal case
    let res = store_client
        .drop_table(DropTablePlan {
            if_exists: true,
            db: "test".to_string(),
            table: "t2".to_string(),
        })
        .await;

    assert!(res.is_ok());

    // normal case
    let res = store_client
        .drop_table(DropTablePlan {
            if_exists: false,
            db: "test".to_string(),
            table: "t1".to_string(),
        })
        .await;
    assert!(res.is_ok());

    Ok(())
}

#[test]
fn test_timeout() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.inject_timeout(Duration::from_secs(6));

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::create(provider);

    // timeout fault only injected for get_database operation
    let res = store_client.get_database("test_db");
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!(e.code(), ErrorCode::Timeout("").code());
    }
    Ok(())
}

#[test]
fn test_invalid_schema() -> common_exception::Result<()> {
    // prepare test data
    let mut fake_apis = FakeStoreApis::new();
    fake_apis.inject_invalid_schema = true;
    fake_apis.insert_db(0, "test");
    fake_apis.insert_tbl("test", TableInfo {
        id: 0,
        ver: 0,
        name: "t1".to_string(),
        schema: Arc::new(DataSchema::empty()),
    });

    let provider = Arc::new(FakeStoreApisProvider::new(fake_apis));
    let store_client = RemoteMetaStoreClient::with_timeout_setting(provider, None);

    let res = store_client.get_all_tables();
    assert!(res.is_err());
    if let Err(e) = res {
        assert_eq!("Code: 1002, displayText = IPC error: Unable to convert flight data to Arrow schema: IPC error: Unable to get root as message.",
                   e.to_string());
    }
    Ok(())
}

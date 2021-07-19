// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_flights::meta_api_impl::CreateDatabaseAction;
use common_flights::meta_api_impl::CreateDatabaseActionResult;
use common_flights::meta_api_impl::CreateTableAction;
use common_flights::meta_api_impl::CreateTableActionResult;
use common_flights::meta_api_impl::DropTableAction;
use common_flights::meta_api_impl::DropTableActionResult;
use common_flights::meta_api_impl::GetDatabaseAction;
use common_flights::meta_api_impl::GetDatabaseActionResult;
use common_flights::meta_api_impl::GetTableAction;
use common_flights::meta_api_impl::GetTableActionResult;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropTablePlan;
use common_planners::TableEngineType;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use common_tracing::tracing;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

use crate::dfs::Dfs;
use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;
use crate::fs::FileSystem;
use crate::localfs::LocalFS;
use crate::meta_service::MetaNode;
use crate::tests::service::new_test_context;
use crate::tests::service::StoreTestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_do_pull_file() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Assert pulling file works fine.

    common_tracing::init_default_tracing();

    let dir = tempdir()?;
    let root = dir.path();

    let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {
        "foo" => "bar",
    })
    .await?;

    {
        // pull file
        let (tx, mut rx): (
            Sender<Result<FlightData, tonic::Status>>,
            Receiver<Result<FlightData, tonic::Status>>,
        ) = tokio::sync::mpsc::channel(2);

        hdlr.do_pull_file("foo".into(), tx).await?;
        let rst = rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("should not be None"))?;
        let rst = rst?;
        let body = rst.data_body;
        assert_eq!("bar", std::str::from_utf8(&body)?);
    }
    Ok(())
}

struct TestDataBase {
    plan: CreateDatabasePlan,
    want: common_exception::Result<CreateDatabaseActionResult>,
}

/// helper to build a D
fn case_db(
    db_name: &str,
    if_not_exists: bool,
    want: common_exception::Result<u64>,
) -> TestDataBase {
    let plan = CreateDatabasePlan {
        db: db_name.to_string(),
        if_not_exists,
        engine: DatabaseEngineType::Local,
        options: Default::default(),
    };
    let want = match want {
        Ok(want_db_id) => Ok(CreateDatabaseActionResult {
            database_id: want_db_id,
        }),
        Err(err) => Err(err), // Result<i64,_> to Result<StoreDoActionResult, _>
    };

    TestDataBase { plan, want }
}

struct TestTable {
    plan: CreateTablePlan,
    want: common_exception::Result<CreateTableActionResult>,
}

/// helper to build a T
fn case_table(
    db_name: &str,
    table_name: &str,
    if_not_exists: bool,
    want: common_exception::Result<u64>,
) -> TestTable {
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let plan = CreateTablePlan {
        if_not_exists,
        db: db_name.to_string(),
        table: table_name.to_string(),
        schema: schema.clone(),
        engine: TableEngineType::JsonEachRaw,
        options: Default::default(),
    };
    let want = match want {
        Ok(want_table_id) => Ok(CreateTableActionResult {
            table_id: want_table_id,
        }),
        Err(err) => Err(err),
    };

    TestTable { plan, want }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_add_database() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert retrieving database.

    common_tracing::init_default_tracing();

    let cases: Vec<TestDataBase> = vec![
        case_db("foo", false, Ok(1)),
        case_db("foo", true, Ok(1)),
        case_db(
            "foo",
            false,
            Err(ErrorCode::DatabaseAlreadyExists("foo database exists")),
        ),
        case_db("bar", true, Ok(2)),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        for (i, c) in cases.iter().enumerate() {
            let mes = format!("{}-th: db plan: {:?}, want: {:?}", i, c.plan, c.want);
            let a = CreateDatabaseAction {
                plan: c.plan.clone(),
            };
            let rst = hdlr.handle(a).await;
            match c.want {
                Ok(ref id) => {
                    assert_eq!(id, &rst.unwrap(), "{}", mes);
                }
                Err(ref status_err) => {
                    let e = rst.unwrap_err();
                    assert_eq!(status_err.code(), e.code(), "{}", mes);
                    assert_eq!(status_err.message(), e.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_get_database() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert getting present and absent databases.

    common_tracing::init_default_tracing();

    struct T {
        db_name: &'static str,
        want: Result<GetDatabaseActionResult, ErrorCode>,
    }

    /// helper to build a T
    fn case(db_name: &'static str, want: Result<u64, &str>) -> T {
        let want = match want {
            Ok(want_db_id) => Ok(GetDatabaseActionResult {
                database_id: want_db_id,
                db: db_name.to_string(),
            }),
            Err(err_str) => Err(ErrorCode::UnknownDatabase(err_str)),
        };

        T { db_name, want }
    }

    let cases: Vec<T> = vec![case("foo", Ok(1)), case("bar", Err("bar"))];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: DatabaseEngineType::Local,
                options: Default::default(),
            };
            let cba = CreateDatabaseAction { plan };
            hdlr.handle(cba).await?;
        }

        for (i, c) in cases.iter().enumerate() {
            let mes = format!("{}-th: db: {:?}, want: {:?}", i, c.db_name, c.want);

            // get db
            let rst = hdlr
                .handle(GetDatabaseAction {
                    db: c.db_name.to_string(),
                })
                .await;

            match c.want {
                Ok(ref act_rst) => {
                    assert_eq!(act_rst, &rst.unwrap(), "{}", mes);
                }
                Err(ref err) => {
                    let got = rst.unwrap_err();
                    let got: ErrorCode = got.into();
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_create_table() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert retrieving database.

    common_tracing::init_default_tracing();

    let db_cases: Vec<TestDataBase> = vec![case_db("foo", false, Ok(1))];
    let table_cases: Vec<TestTable> = vec![
        case_table("foo", "foo_t1", false, Ok(1)),
        case_table("foo", "foo_t1", true, Ok(1)),
        case_table(
            "foo",
            "foo_t1",
            false,
            Err(ErrorCode::TableAlreadyExists("table exists: foo_t1")),
        ),
        case_table("foo", "foo_t2", true, Ok(2)),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        for (i, c) in db_cases.iter().enumerate() {
            let mes = format!("{}-th: db plan: {:?}, want: {:?}", i, c.plan, c.want);
            let a = CreateDatabaseAction {
                plan: c.plan.clone(),
            };
            let rst = hdlr.handle(a).await;
            match c.want {
                Ok(ref id) => {
                    assert_eq!(id, &rst.unwrap(), "{}", mes);
                }
                Err(ref status_err) => {
                    let e = rst.unwrap_err();
                    assert_eq!(status_err.code(), e.code(), "{}", mes);
                    assert_eq!(status_err.message(), e.message(), "{}", mes);
                }
            }
        }

        for (i, t) in table_cases.iter().enumerate() {
            let mes = format!("{}-th: table plan: {:?}, want: {:?}", i, t.plan, t.want);
            let a = CreateTableAction {
                plan: t.plan.clone(),
            };
            let rst = hdlr.handle(a).await;
            match t.want {
                Ok(ref id) => {
                    assert_eq!(id, &rst.unwrap(), "{}", mes);
                }
                Err(ref status_err) => {
                    let e = rst.unwrap_err();
                    assert_eq!(status_err.code(), e.code(), "{}", mes);
                    assert_eq!(status_err.message(), e.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_get_table() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert getting present and absent databases.

    common_tracing::init_default_tracing();

    struct T {
        db_name: &'static str,
        table_name: &'static str,
        want: Result<GetTableActionResult, ErrorCode>,
    }

    /// helper to build a T
    fn case(db_name: &'static str, table_name: &'static str, want: Result<u64, &str>) -> T {
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false,
        )]));

        let want = match want {
            Ok(want_table_id) => Ok(GetTableActionResult {
                table_id: want_table_id,
                db: db_name.to_string(),
                name: table_name.to_string(),
                schema: schema.clone(),
            }),
            Err(err_str) => Err(ErrorCode::UnknownTable(err_str)),
        };

        T {
            db_name,
            table_name,
            want,
        }
    }

    let table_cases: Vec<T> = vec![
        case("foo", "foo_t1", Ok(1)),
        case("foo", "foo_t2", Err("table not found: foo_t2")),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: DatabaseEngineType::Local,
                options: Default::default(),
            };
            let cba = CreateDatabaseAction { plan };
            hdlr.handle(cba).await?;
        }

        {
            // create table
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )]));

            let plan = CreateTablePlan {
                if_not_exists: false,
                db: "foo".to_string(),
                table: "foo_t1".to_string(),
                schema: schema.clone(),
                engine: TableEngineType::JsonEachRaw,
                options: Default::default(),
            };
            let cta = CreateTableAction { plan };
            hdlr.handle(cta).await?;
        }

        for (i, c) in table_cases.iter().enumerate() {
            let mes = format!(
                "{}-th: db-table: {:?}-{:?}, want: {:?}",
                i, c.db_name, c.table_name, c.want
            );

            // get db
            let rst = hdlr
                .handle(GetTableAction {
                    db: c.db_name.to_string(),
                    table: c.table_name.to_string(),
                })
                .await;

            match c.want {
                Ok(ref act_rst) => {
                    assert_eq!(act_rst, &rst.unwrap(), "{}", mes);
                }
                Err(ref err) => {
                    let got = rst.unwrap_err();
                    let got: ErrorCode = got.into();
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_drop_table() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert getting present and absent databases.

    common_tracing::init_default_tracing();

    struct T {
        db_name: &'static str,
        table_name: &'static str,
        if_exists: bool,
        want: Result<DropTableActionResult, ErrorCode>,
    }

    /// helper to build a T
    fn case(
        db_name: &'static str,
        table_name: &'static str,
        if_exists: bool,
        want: Result<(), &str>,
    ) -> T {
        let want = match want {
            Ok(..) => Ok(DropTableActionResult {}),
            Err(err_str) => Err(ErrorCode::UnknownTable(err_str)),
        };

        T {
            db_name,
            table_name,
            if_exists,
            want,
        }
    }

    let table_cases: Vec<T> = vec![
        case("foo", "foo_t1", false, Ok(())),
        case("foo", "foo_t1", true, Ok(())),
        case("foo", "foo_t1", false, Err("table not found: foo_t1")),
        case("foo", "foo_t2", true, Ok(())),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let (_tc, hdlr) = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: DatabaseEngineType::Local,
                options: Default::default(),
            };
            let cba = CreateDatabaseAction { plan };
            hdlr.handle(cba).await?;
        }

        {
            // create table
            let schema = Arc::new(DataSchema::new(vec![DataField::new(
                "number",
                DataType::UInt64,
                false,
            )]));

            let plan = CreateTablePlan {
                if_not_exists: false,
                db: "foo".to_string(),
                table: "foo_t1".to_string(),
                schema: schema.clone(),
                engine: TableEngineType::JsonEachRaw,
                options: Default::default(),
            };
            let cta = CreateTableAction { plan };
            hdlr.handle(cta).await?;
        }

        for (i, c) in table_cases.iter().enumerate() {
            let mes = format!(
                "{}-th: db-table: {:?}-{:?}, want: {:?}",
                i, c.db_name, c.table_name, c.want
            );

            let rst = hdlr
                .handle(DropTableAction {
                    plan: DropTablePlan {
                        if_exists: c.if_exists,
                        db: c.db_name.to_string(),
                        table: c.table_name.to_string(),
                    },
                })
                .await;

            match c.want {
                Ok(ref act_rst) => {
                    assert_eq!(act_rst, &rst.unwrap(), "{}", mes);
                }
                Err(ref err) => {
                    let got = rst.unwrap_err();
                    let got: ErrorCode = got.into();
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

// Start an ActionHandler backed with a dfs.
// And feed files into dfs.
async fn bring_up_dfs_action_handler(
    root: &Path,
    files: HashMap<&str, &str>,
) -> anyhow::Result<(StoreTestContext, ActionHandler)> {
    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let mut tc = new_test_context();

    let mn = MetaNode::boot(0, &tc.config).await?;
    tc.meta_nodes.push(mn.clone());

    let dfs = Dfs::create(fs, mn.clone());

    for (key, content) in files.iter() {
        dfs.add((*key).into(), (*content).as_bytes()).await?;
        tracing::debug!("dfs added file: {} {:?}", *key, *content);
    }

    let ah = ActionHandler::create(Arc::new(dfs), mn);

    Ok((tc, ah))
}

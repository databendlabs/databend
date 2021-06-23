// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
use common_exception::ErrorCode;
use common_flights::CreateDatabaseAction;
use common_flights::GetDatabaseAction;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use common_store_api::CreateDatabaseActionResult;
use common_store_api::GetDatabaseActionResult;
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
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_do_pull_file() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Assert pulling file works fine.

    common_tracing::init_default_tracing();

    let dir = tempdir()?;
    let root = dir.path();

    let hdlr = bring_up_dfs_action_handler(root, hashmap! {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_add_database() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert retrieving database.

    common_tracing::init_default_tracing();

    struct T {
        plan: CreateDatabasePlan,
        want: common_exception::Result<CreateDatabaseActionResult>,
    }

    /// helper to build a T
    fn case(dbname: &str, if_not_exists: bool, want: common_exception::Result<i64>) -> T {
        let plan = CreateDatabasePlan {
            db: dbname.to_string(),
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

        T { plan, want }
    }

    // TODO: id should be started from 1
    let cases: Vec<T> = vec![
        case("foo", false, Ok(0)),
        case("foo", true, Ok(0)),
        case(
            "foo",
            false,
            Err(ErrorCode::DatabaseAlreadExists("foo database exists")),
        ),
        case("bar", true, Ok(1)),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let hdlr = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        for (i, c) in cases.iter().enumerate() {
            let mes = format!("{}-th: plan: {:?}, want: {:?}", i, c.plan, c.want);
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
    fn case(db_name: &'static str, want: Result<i64, &str>) -> T {
        let want = match want {
            Ok(want_db_id) => Ok(GetDatabaseActionResult {
                database_id: want_db_id,
                db: db_name.to_string(),
            }),
            Err(err_str) => Err(ErrorCode::UnknownDatabase(err_str)),
        };

        T { db_name, want }
    }

    // TODO: id should be started from 1
    let cases: Vec<T> = vec![case("foo", Ok(0)), case("bar", Err("bar"))];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let hdlr = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: DatabaseEngineType::Local,
                options: Default::default(),
            };
            let cba = CreateDatabaseAction { plan: plan };
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

// Start an ActionHandler backed with a dfs.
// And feed files into dfs.
async fn bring_up_dfs_action_handler(
    root: &Path,
    files: HashMap<&str, &str>,
) -> anyhow::Result<ActionHandler> {
    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let meta_addr = rand_local_addr();
    let mn = MetaNode::boot(0, meta_addr.clone()).await?;

    let dfs = Dfs::create(fs, mn.clone());

    for (key, content) in files.iter() {
        dfs.add((*key).into(), (*content).as_bytes()).await?;
        tracing::debug!("dfs added file: {} {:?}", *key, *content);
    }

    let ah = ActionHandler::create(Arc::new(dfs), mn);

    Ok(ah)
}

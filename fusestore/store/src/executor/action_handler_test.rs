// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
use common_flights::CreateDatabaseAction;
use common_flights::CreateDatabaseActionResult;
use common_flights::StoreDoAction;
use common_flights::StoreDoActionResult;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use common_tracing::tracing;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use tempfile::tempdir;
use tonic::Status;

use crate::dfs::Dfs;
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
        want: Result<StoreDoActionResult, Status>,
    }

    /// helper to build a T
    fn case(dbname: &str, if_not_exists: bool, want: Result<i64, &str>) -> T {
        let plan = CreateDatabasePlan {
            db: dbname.to_string(),
            if_not_exists,
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };
        let want = match want {
            Ok(want_db_id) => Ok(StoreDoActionResult::CreateDatabase(
                CreateDatabaseActionResult {
                    database_id: want_db_id,
                },
            )),
            Err(err_str) => Err(Status::internal(err_str)),
        };

        T { plan, want }
    }

    // TODO: id should be started from 1
    let cases: Vec<T> = vec![
        case("foo", false, Ok(0)),
        case("foo", true, Ok(0)),
        case("foo", false, Err("foo database exists")),
        case("bar", true, Ok(1)),
    ];

    {
        let dir = tempdir()?;
        let root = dir.path();
        let hdlr = bring_up_dfs_action_handler(root, hashmap! {}).await?;

        for (i, c) in cases.iter().enumerate() {
            let mes = format!("{}-th: plan: {:?}, want: {:?}", i, c.plan, c.want);
            let cba = CreateDatabaseAction {
                plan: c.plan.clone(),
            };
            let a = StoreDoAction::CreateDatabase(cba);
            let rst = hdlr.execute(a).await;
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

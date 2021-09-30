// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
use common_base::tokio;
use common_base::tokio::sync::mpsc::Receiver;
use common_base::tokio::sync::mpsc::Sender;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_meta_api_vo::*;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_store_api_sdk::meta_api_impl::CreateDatabaseAction;
use common_store_api_sdk::meta_api_impl::CreateTableAction;
use common_store_api_sdk::meta_api_impl::DropDatabaseAction;
use common_store_api_sdk::meta_api_impl::DropTableAction;
use common_store_api_sdk::meta_api_impl::GetDatabaseAction;
use common_store_api_sdk::meta_api_impl::GetTableAction;
use common_store_api_sdk::storage_api_impl::AppendResult;
use common_store_api_sdk::storage_api_impl::TruncateTableAction;
use common_store_api_sdk::storage_api_impl::TruncateTableResult;
use common_tracing::tracing;
use kvsrv::meta_service::MetaNode;
use maplit::hashmap;
use pretty_assertions::assert_eq;

use crate::dfs::Dfs;
use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;
use crate::fs::FileSystem;
use crate::localfs::LocalFS;
use crate::tests::service::new_test_context;
use crate::tests::service::StoreTestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_do_pull_file() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Assert pulling file works fine.

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {
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

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    struct D {
        plan: CreateDatabasePlan,
        want: common_exception::Result<CreateDatabaseActionResult>,
    }

    /// helper to build a D
    fn case_db(db_name: &str, if_not_exists: bool, want: common_exception::Result<u64>) -> D {
        let plan = CreateDatabasePlan {
            db: db_name.to_string(),
            if_not_exists,
            engine: "Local".to_string(),
            options: Default::default(),
        };
        let want = match want {
            Ok(want_db_id) => Ok(CreateDatabaseActionResult {
                database_id: want_db_id,
            }),
            Err(err) => Err(err), // Result<i64,_> to Result<StoreDoActionResult, _>
        };

        D { plan, want }
    }

    let cases: Vec<D> = vec![
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
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

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

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    struct T {
        db_name: &'static str,
        want: Result<DatabaseInfo, ErrorCode>,
    }

    /// helper to build a T
    fn case(db_name: &'static str, want: Result<u64, &str>) -> T {
        let want = match want {
            Ok(want_db_id) => Ok(DatabaseInfo {
                database_id: want_db_id,
                db: db_name.to_string(),
                engine: "Local".to_string(),
            }),
            Err(err_str) => Err(ErrorCode::UnknownDatabase(err_str)),
        };

        T { db_name, want }
    }

    let cases: Vec<T> = vec![case("foo", Ok(1)), case("bar", Err("bar"))];

    {
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: "Local".to_string(),
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
                    let got: ErrorCode = got;
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_drop_database() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a database.
    // - Assert getting present and absent databases.

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    struct T {
        db_name: &'static str,
        if_exists: bool,
        want: Result<DropDatabaseActionResult, ErrorCode>,
    }

    /// helper to build a T
    fn case(db_name: &'static str, if_exists: bool, want: Result<(), &str>) -> T {
        let want = match want {
            Ok(..) => Ok(DropDatabaseActionResult {}),
            Err(err_str) => Err(ErrorCode::UnknownDatabase(err_str)),
        };

        T {
            db_name,
            if_exists,
            want,
        }
    }

    let db_cases: Vec<T> = vec![
        case("foo", false, Ok(())),
        case("foo", true, Ok(())),
        case("foo", false, Err("database not found: foo")),
        case("foo", true, Ok(())),
    ];

    {
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: "Local".to_string(),
                options: Default::default(),
            };
            let cba = CreateDatabaseAction { plan };
            hdlr.handle(cba).await?;
        }

        for (i, c) in db_cases.iter().enumerate() {
            let mes = format!("{}-th: db: {:?}, want: {:?}", i, c.db_name, c.want);

            let rst = hdlr
                .handle(DropDatabaseAction {
                    plan: DropDatabasePlan {
                        if_exists: c.if_exists,
                        db: c.db_name.to_string(),
                    },
                })
                .await;

            match c.want {
                Ok(ref act_rst) => {
                    assert_eq!(act_rst, &rst.unwrap(), "{}", mes);
                }
                Err(ref err) => {
                    let got = rst.unwrap_err();
                    let got: ErrorCode = got;
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

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    struct D {
        plan: CreateDatabasePlan,
        want: common_exception::Result<CreateDatabaseActionResult>,
    }

    /// helper to build a D
    fn case_db(db_name: &str, if_not_exists: bool, want: common_exception::Result<u64>) -> D {
        let plan = CreateDatabasePlan {
            db: db_name.to_string(),
            if_not_exists,
            engine: "Local".to_string(),
            options: Default::default(),
        };
        let want = match want {
            Ok(want_db_id) => Ok(CreateDatabaseActionResult {
                database_id: want_db_id,
            }),
            Err(err) => Err(err), // Result<i64,_> to Result<StoreDoActionResult, _>
        };

        D { plan, want }
    }

    struct T {
        plan: CreateTablePlan,
        want: common_exception::Result<CreateTableActionResult>,
    }

    /// helper to build a T
    fn case_table(
        db_name: &str,
        table_name: &str,
        if_not_exists: bool,
        want: common_exception::Result<u64>,
    ) -> T {
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false,
        )]));
        let plan = CreateTablePlan {
            if_not_exists,
            db: db_name.to_string(),
            table: table_name.to_string(),
            schema,
            engine: "JSON".to_string(),
            options: Default::default(),
        };
        let want = match want {
            Ok(want_table_id) => Ok(CreateTableActionResult {
                table_id: want_table_id,
            }),
            Err(err) => Err(err),
        };

        T { plan, want }
    }

    let db_cases: Vec<D> = vec![case_db("foo", false, Ok(1))];
    let table_cases: Vec<T> = vec![
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
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

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

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

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
                schema,
                engine: "JSON".to_owned(),
                options: Default::default(),
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
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: "Local".to_string(),
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
                engine: "JSON".to_string(),
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
                    let got: ErrorCode = got;
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

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

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
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: "Local".to_string(),
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
                engine: "JSON".to_string(),
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
                    let got: ErrorCode = got;
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_truncate_table() -> anyhow::Result<()> {
    // - Bring up an ActionHandler backed with a Dfs
    // - Add a table.
    // - Assert getting present and absent databases.

    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    struct T {
        db_name: &'static str,
        table_name: &'static str,
        want: Result<TruncateTableResult, ErrorCode>,
    }

    /// helper to build a T
    fn case(db_name: &'static str, table_name: &'static str, want: Result<(), &str>) -> T {
        let want = match want {
            Ok(..) => Ok(TruncateTableResult {
                truncated_table_data_parts_count: 1,
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
        case("foo", "foo_t1", Ok(())),
        case("foo", "foo_t2", Err("table not found: foo_t2")),
    ];

    {
        let (_tc, hdlr) = bring_up_dfs_action_handler(hashmap! {}).await?;

        {
            // create db
            let plan = CreateDatabasePlan {
                db: "foo".to_string(),
                if_not_exists: false,
                engine: "Local".to_string(),
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
                engine: "JSON".to_string(),
                options: Default::default(),
            };
            let cta = CreateTableAction { plan };
            hdlr.handle(cta).await?;
        }

        // append fake parts for test
        let mut append_result = AppendResult::default();
        let location = format!("{}/{}", "path", "part_uuid");
        append_result.append_part(&location, 1, 1, 1, 1);
        hdlr.meta_node
            .append_data_parts("foo", "foo_t1", &append_result)
            .await;
        let mut before_parts_len: usize = 0;
        let before_parts = hdlr.meta_node.get_data_parts("foo", "foo_t1").await;
        if let Some(before_parts) = before_parts {
            before_parts_len = before_parts.len();
        }
        assert_eq!(1, before_parts_len);

        for (i, c) in table_cases.iter().enumerate() {
            let mes = format!(
                "{}-th: db-table: {:?}-{:?}, want: {:?}",
                i, c.db_name, c.table_name, c.want
            );

            let rst = hdlr
                .handle(TruncateTableAction {
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
                    let got: ErrorCode = got;
                    assert_eq!(err.code(), got.code(), "{}", mes);
                    assert_eq!(err.message(), got.message(), "{}", mes);
                }
            }
        }
        let mut after_parts_len: usize = 0;
        let after_parts = hdlr.meta_node.get_data_parts("foo", "foo_t1").await;
        if let Some(after_parts) = after_parts {
            after_parts_len = after_parts.len();
        }
        assert_eq!(0, after_parts_len);
    }

    Ok(())
}

// Start an ActionHandler backed with a dfs.
// And feed files into dfs.
async fn bring_up_dfs_action_handler(
    files: HashMap<&str, &str>,
) -> anyhow::Result<(StoreTestContext, ActionHandler)> {
    let mut tc = new_test_context();
    let fs = LocalFS::try_create(tc.config.local_fs_dir.clone())?;

    let mn = MetaNode::boot(0, &tc.config.meta_config).await?;
    tc.meta_nodes.push(mn.clone());

    let dfs = Dfs::create(fs, mn.clone());

    for (key, content) in files.iter() {
        dfs.add(*key, (*content).as_bytes()).await?;
        tracing::debug!("dfs added file: {} {:?}", *key, *content);
    }

    let ah = ActionHandler::create(Arc::new(dfs), mn);

    Ok((tc, ah))
}

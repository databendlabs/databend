// Copyright 2021 Datafuse Labs
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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Duration;
use chrono::Utc;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::ScheduleOptions as AstScheduleOptions;
use databend_common_ast::ast::TaskSql;
use databend_common_exception::Result;
use databend_common_management::TaskMgr;
use databend_common_meta_app::principal::ScheduleOptions;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::WarehouseOptions;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::task::TaskSql as MetaTaskSql;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_runtime::DatabendRuntime;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_task_assigns_task_id() -> anyhow::Result<()> {
    let (_kv, task_mgr) = new_task_api().await?;

    task_mgr
        .create_task(test_task("task_1", "select 1"), &CreateOption::Create)
        .await??;
    task_mgr
        .create_task(test_task("task_2", "select 2"), &CreateOption::Create)
        .await??;

    let task_1 = task_mgr.describe_task("task_1").await??.unwrap();
    let task_2 = task_mgr.describe_task("task_2").await??.unwrap();

    assert_ne!(task_1.task_id, EMPTY_TASK_ID);
    assert_ne!(task_2.task_id, EMPTY_TASK_ID);
    assert_ne!(task_1.task_id, task_2.task_id);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_or_replace_task_assigns_new_task_id() -> anyhow::Result<()> {
    let (_kv, task_mgr) = new_task_api().await?;

    task_mgr
        .create_task(test_task("task_1", "select 1"), &CreateOption::Create)
        .await??;
    let old_task = task_mgr.describe_task("task_1").await??.unwrap();

    task_mgr
        .create_task(
            test_task("task_1", "select 2"),
            &CreateOption::CreateOrReplace,
        )
        .await??;
    let new_task = task_mgr.describe_task("task_1").await??.unwrap();

    assert_ne!(new_task.task_id, EMPTY_TASK_ID);
    assert_ne!(new_task.task_id, old_task.task_id);
    assert_eq!(new_task.task_sql, MetaTaskSql::Sql("select 2".to_string()));

    Ok(())
}

#[test]
fn test_make_task_sql_preserves_script_sql() {
    let sql = TaskSql::ScriptBlock(vec![
        "INSERT INTO t VALUES(1)".to_string(),
        "INSERT INTO t VALUES(2)".to_string(),
    ]);

    assert_eq!(
        TaskMgr::make_task_sql(&sql),
        MetaTaskSql::Script(vec![
            "INSERT INTO t VALUES(1)".to_string(),
            "INSERT INTO t VALUES(2)".to_string()
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alter_task_set_preserves_unset_options() -> anyhow::Result<()> {
    let (_kv, task_mgr) = new_task_api().await?;
    let mut task = test_task("task_1", "select 1");
    task.schedule_options = Some(ScheduleOptions {
        interval: Some(60),
        cron: None,
        time_zone: None,
        schedule_type: ScheduleType::IntervalType,
        milliseconds_interval: None,
    });
    task.comment = Some("original comment".to_string());
    task.suspend_task_after_num_failures = Some(3);
    task.error_integration = Some("original_error_integration".to_string());
    task.session_params
        .insert("max_threads".to_string(), "2".to_string());

    task_mgr.create_task(task, &CreateOption::Create).await??;

    task_mgr
        .alter_task("task_1", &AlterTaskOptions::Set {
            warehouse: Some("new_warehouse".to_string()),
            schedule: None,
            suspend_task_after_num_failures: None,
            comments: None,
            session_parameters: None,
            error_integration: None,
        })
        .await??;

    let task = task_mgr.describe_task("task_1").await??.unwrap();
    assert_eq!(
        task.warehouse_options,
        Some(WarehouseOptions {
            warehouse: Some("new_warehouse".to_string()),
            using_warehouse_size: None,
        })
    );
    assert_eq!(
        task.schedule_options,
        Some(ScheduleOptions {
            interval: Some(60),
            cron: None,
            time_zone: None,
            schedule_type: ScheduleType::IntervalType,
            milliseconds_interval: None,
        })
    );
    assert_eq!(task.comment, Some("original comment".to_string()));
    assert_eq!(task.suspend_task_after_num_failures, Some(3));
    assert_eq!(
        task.error_integration,
        Some("original_error_integration".to_string())
    );
    assert_eq!(
        task.session_params.get("max_threads").map(String::as_str),
        Some("2")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alter_task_set_schedule_preserves_warehouse() -> anyhow::Result<()> {
    let (_kv, task_mgr) = new_task_api().await?;

    task_mgr
        .create_task(test_task("task_1", "select 1"), &CreateOption::Create)
        .await??;

    task_mgr
        .alter_task("task_1", &AlterTaskOptions::Set {
            warehouse: None,
            schedule: Some(AstScheduleOptions::IntervalSecs(30, 0)),
            suspend_task_after_num_failures: None,
            comments: None,
            session_parameters: None,
            error_integration: None,
        })
        .await??;

    let task = task_mgr.describe_task("task_1").await??.unwrap();
    assert_eq!(
        task.warehouse_options,
        Some(WarehouseOptions {
            warehouse: Some("default".to_string()),
            using_warehouse_size: None,
        })
    );
    assert_eq!(
        task.schedule_options,
        Some(ScheduleOptions {
            interval: Some(30),
            cron: None,
            time_zone: None,
            schedule_type: ScheduleType::IntervalType,
            milliseconds_interval: None,
        })
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_update_does_not_overwrite_alter() -> anyhow::Result<()> {
    let (_kv, task_mgr) = new_task_api().await?;
    let mut task = test_task("task_1", "select 1");
    task.status = Status::Started;
    task.schedule_options = Some(ScheduleOptions {
        interval: Some(60),
        cron: None,
        time_zone: None,
        schedule_type: ScheduleType::IntervalType,
        milliseconds_interval: None,
    });
    task_mgr.create_task(task, &CreateOption::Create).await??;

    let mut stale_task = task_mgr.describe_task("task_1").await??.unwrap();
    task_mgr
        .alter_task(
            "task_1",
            &AlterTaskOptions::ModifyAs(TaskSql::SingleStatement("select 2".to_string())),
        )
        .await??;

    let next_scheduled_at = Utc::now() + Duration::minutes(1);
    stale_task.next_scheduled_at = Some(next_scheduled_at);
    stale_task.updated_at = Utc::now();
    assert!(task_mgr.update_task(stale_task.clone()).await??);

    let current_task = task_mgr.describe_task("task_1").await??.unwrap();
    assert_eq!(
        current_task.task_sql,
        MetaTaskSql::Sql("select 2".to_string())
    );
    assert_eq!(current_task.next_scheduled_at, Some(next_scheduled_at));

    task_mgr
        .alter_task("task_1", &AlterTaskOptions::Suspend)
        .await??;
    assert!(!task_mgr.update_task(stale_task).await??);
    assert_eq!(
        task_mgr.describe_task("task_1").await??.unwrap().status,
        Status::Suspended
    );

    Ok(())
}

fn test_task(name: &str, query_text: &str) -> Task {
    let now = Utc::now();

    Task {
        task_id: EMPTY_TASK_ID,
        task_name: name.to_string(),
        task_sql: MetaTaskSql::Sql(query_text.to_string()),
        when_condition: None,
        after: vec![],
        comment: None,
        owner: "account_admin".to_string(),
        owner_user: "root".to_string(),
        schedule_options: None,
        warehouse_options: Some(WarehouseOptions {
            warehouse: Some("default".to_string()),
            using_warehouse_size: None,
        }),
        next_scheduled_at: None,
        suspend_task_after_num_failures: None,
        error_integration: None,
        status: Status::Suspended,
        created_at: now,
        updated_at: now,
        last_suspended_at: None,
        session_params: BTreeMap::new(),
    }
}

async fn new_task_api() -> Result<(Arc<MetaStore>, TaskMgr)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let test_api = Arc::new(test_api);

    let mgr = TaskMgr::create(test_api.clone(), &Tenant::new_literal("admin"));
    Ok((test_api, mgr))
}

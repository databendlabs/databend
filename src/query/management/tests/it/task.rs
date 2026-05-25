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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_management::TaskMgr;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::WarehouseOptions;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
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
    assert_eq!(new_task.query_text, "select 2");

    Ok(())
}

fn test_task(name: &str, query_text: &str) -> Task {
    let now = Utc::now();

    Task {
        task_id: EMPTY_TASK_ID,
        task_name: name.to_string(),
        query_text: query_text.to_string(),
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

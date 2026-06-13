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

use std::collections::HashMap;
use std::time::Duration;

use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_exception::ErrorCode;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_meta_store::MetaStore;
use databend_meta_runtime::DatabendRuntime;

async fn create_workload_mgr() -> WorkloadMgr {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    WorkloadMgr::create(test_api.clone(), "test-tenant-id").unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_workload_group() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;
    let mut quotas = HashMap::new();
    quotas.insert(
        "cpu".to_string(),
        QuotaValue::Duration(Duration::from_secs(10)),
    );

    let group = WorkloadGroup {
        id: "".to_string(),
        name: "test_group".to_string(),
        quotas,
    };

    // Test successful creation
    let created = mgr.create(group).await?;
    assert!(!created.id.is_empty());
    assert_eq!(created.name, "test_group");
    assert_eq!(created.quotas.len(), 1);

    // Test duplicate creation
    let mut quotas = HashMap::new();
    quotas.insert(
        "cpu".to_string(),
        QuotaValue::Duration(Duration::from_secs(10)),
    );

    let group = WorkloadGroup {
        id: "".to_string(),
        name: "test_group".to_string(),
        quotas,
    };

    let result = mgr.create(group).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::AlreadyExistsWorkload("").code()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_workload_group() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;
    let mut quotas = HashMap::new();
    quotas.insert(
        "cpu".to_string(),
        QuotaValue::Duration(Duration::from_secs(10)),
    );

    let group = WorkloadGroup {
        id: "".to_string(),
        name: "test_group".to_string(),
        quotas,
    };

    let created = mgr.create(group).await?;
    let id = created.id.clone();

    // Test get by id
    let by_id = mgr.get_by_id(&id).await?;
    assert_eq!(by_id.name, "test_group");

    // Test get by name
    let by_name = mgr.get_by_name("test_group").await?;
    assert_eq!(by_name.id, id);

    // Test get unknown workload
    assert!(mgr.get_by_name("unknown").await.is_err());
    assert!(mgr.get_by_id("unknown").await.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_workload_group() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;
    let group = WorkloadGroup {
        id: "".to_string(),
        name: "test_group".to_string(),
        quotas: HashMap::new(),
    };

    let created = mgr.create(group).await?;
    let name = created.name.clone();

    // Test successful drop
    mgr.drop(name.clone()).await?;
    assert!(mgr.get_by_name(&name).await.is_err());

    // Test drop unknown workload
    assert!(mgr.drop("unknown".to_string()).await.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rename_workload_group() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;
    let group = WorkloadGroup {
        id: "".to_string(),
        name: "old_name".to_string(),
        quotas: HashMap::new(),
    };

    let created = mgr.create(group).await?;
    let id = created.id.clone();

    // Test successful rename
    mgr.rename("old_name".to_string(), "new_name".to_string())
        .await?;

    // Verify old name doesn't exist
    assert!(mgr.get_by_name("old_name").await.is_err());

    // Verify new name exists
    let renamed = mgr.get_by_name("new_name").await?;
    assert_eq!(renamed.id, id);

    // Test rename to existing name
    let group = WorkloadGroup {
        id: "".to_string(),
        name: "another_group".to_string(),
        quotas: HashMap::new(),
    };
    mgr.create(group).await?;

    let result = mgr
        .rename("new_name".to_string(), "another_group".to_string())
        .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::InvalidWorkload("").code()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alter_quotas() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;
    let group = WorkloadGroup {
        id: "".to_string(),
        name: "test_group".to_string(),
        quotas: HashMap::new(),
    };

    mgr.create(group).await?;

    // Test adding new quota
    let mut new_quotas = HashMap::new();
    new_quotas.insert(
        "cpu".to_string(),
        QuotaValue::Duration(Duration::from_secs(10)),
    );

    mgr.set_quotas("test_group".to_string(), new_quotas).await?;

    let updated = mgr.get_by_name("test_group").await?;
    assert_eq!(updated.quotas.len(), 1);
    assert!(matches!(
        updated.quotas.get("cpu").unwrap(),
        QuotaValue::Duration(_)
    ));

    // Test updating existing quota
    let mut update_quotas = HashMap::new();
    update_quotas.insert(
        "cpu".to_string(),
        QuotaValue::Duration(Duration::from_secs(20)),
    );

    mgr.set_quotas("test_group".to_string(), update_quotas)
        .await?;

    let updated = mgr.get_by_name("test_group").await?;
    let duration = match updated.quotas.get("cpu").unwrap() {
        QuotaValue::Duration(d) => d,
        _ => panic!("Expected Duration"),
    };
    assert_eq!(duration.as_secs(), 20);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_all_workload_groups() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;

    // Create multiple groups
    let group1 = WorkloadGroup {
        id: "".to_string(),
        name: "group1".to_string(),
        quotas: HashMap::new(),
    };
    let group2 = WorkloadGroup {
        id: "".to_string(),
        name: "group2".to_string(),
        quotas: HashMap::new(),
    };

    mgr.create(group1).await?;
    mgr.create(group2).await?;

    let all_groups = mgr.get_all().await?;
    assert_eq!(all_groups.len(), 2);
    assert!(all_groups.iter().any(|g| g.name == "group1"));
    assert!(all_groups.iter().any(|g| g.name == "group2"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_invalid_workload_group() -> anyhow::Result<()> {
    let mgr = create_workload_mgr().await;

    // Test empty name
    let group = WorkloadGroup {
        id: "".to_string(),
        name: "".to_string(),
        quotas: HashMap::new(),
    };
    let result = mgr.create(group).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::InvalidWorkload("").code()
    );

    // Test non-empty id
    let group = WorkloadGroup {
        id: "some_id".to_string(),
        name: "test".to_string(),
        quotas: HashMap::new(),
    };
    let result = mgr.create(group).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        ErrorCode::InvalidWorkload("").code()
    );

    Ok(())
}

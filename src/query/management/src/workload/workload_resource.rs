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
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;

use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_base::runtime::MemStat;
use databend_common_exception::Result;

use crate::workload::workload_mgr::WorkloadMgr;
use crate::WorkloadApi;

pub struct WorkloadGroupResource {
    workload: WorkloadGroup,
    pub mem_stat: Arc<MemStat>,
    mgr: Weak<WorkloadGroupResourceManagerInner>,
}

impl Drop for WorkloadGroupResource {
    fn drop(&mut self) {
        if let Some(resource_manager) = self.mgr.upgrade() {
            resource_manager.remove_workload(&self.workload.id);
        }
    }
}

impl Deref for WorkloadGroupResource {
    type Target = WorkloadGroup;

    fn deref(&self) -> &Self::Target {
        &self.workload
    }
}

struct WorkloadGroupResourceManagerInner {
    workload_mgr: Arc<WorkloadMgr>,
    online_workload_group: Mutex<HashMap<String, Weak<WorkloadGroupResource>>>,
}

pub struct WorkloadGroupResourceManager {
    inner: Arc<WorkloadGroupResourceManagerInner>,
}

impl WorkloadGroupResourceManagerInner {
    pub fn new(workload_mgr: Arc<WorkloadMgr>) -> Arc<WorkloadGroupResourceManagerInner> {
        Arc::new(WorkloadGroupResourceManagerInner {
            workload_mgr,
            online_workload_group: Mutex::new(HashMap::new()),
        })
    }

    fn cleanup_expired(&self) {
        let mut online_workload_group = self
            .online_workload_group
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        online_workload_group.retain(|_, v| v.upgrade().is_some());
    }

    fn remove_workload(&self, id: &str) {
        let mut online_workload_group = self
            .online_workload_group
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        online_workload_group.remove(id);
    }

    pub async fn get_workload(self: &Arc<Self>, id: &str) -> Result<Arc<WorkloadGroupResource>> {
        self.cleanup_expired();

        {
            let online_workload_group = self
                .online_workload_group
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(online_workload_group) = online_workload_group.get(id) {
                if let Some(workload) = online_workload_group.upgrade() {
                    return Ok(workload);
                }
            }
        }

        let workload = self.workload_mgr.get_by_id(id).await?;

        let mut online_workload_group = self
            .online_workload_group
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(online_workload_group) = online_workload_group.get(id) {
            if let Some(workload) = online_workload_group.upgrade() {
                return Ok(workload);
            }
        }

        let workload_resource = Arc::new(WorkloadGroupResource {
            workload,
            mem_stat: MemStat::create_workload_group(),
            mgr: Arc::downgrade(self),
        });

        online_workload_group.insert(
            workload_resource.workload.id.clone(),
            Arc::downgrade(&workload_resource),
        );

        // TODO: reset limit

        Ok(workload_resource)
    }
}

impl WorkloadGroupResourceManager {
    pub fn new(workload_mgr: Arc<WorkloadMgr>) -> Arc<Self> {
        Arc::new(WorkloadGroupResourceManager {
            inner: WorkloadGroupResourceManagerInner::new(workload_mgr),
        })
    }

    pub async fn get_workload(&self, id: &str) -> Result<Arc<WorkloadGroupResource>> {
        self.inner.get_workload(id).await
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::base::tokio;
    use databend_common_meta_store::MetaStore;

    use super::*;

    async fn create_workload_mgr() -> WorkloadMgr {
        let test_api = MetaStore::new_local_testing().await;
        WorkloadMgr::create(test_api.clone(), "test-tenant-id").unwrap()
    }

    #[tokio::test]
    async fn test_workload_resource() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone());

        let workload = WorkloadGroup {
            id: String::new(),
            name: "test_workload".to_string(),
            quotas: HashMap::new(),
        };

        let workload = workload_mgr.create(workload).await?;

        {
            let workload1 = inner.get_workload(&workload.id).await?;
            assert_eq!(workload1.id, workload.id);
            assert_eq!(workload1.name, "test_workload");

            let workload2 = inner.get_workload(&workload.id).await?;
            assert_eq!(Arc::as_ptr(&workload1), Arc::as_ptr(&workload2));

            assert_eq!(inner.online_workload_group.lock().unwrap().len(), 1);
        }

        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 0);

        Ok(())
    }
}

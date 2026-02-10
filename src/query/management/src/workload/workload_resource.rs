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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::workload_group::MAX_CONCURRENCY_QUOTA_KEY;
use databend_common_base::runtime::workload_group::MEMORY_QUOTA_KEY;
use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroupResource;
use databend_common_exception::Result;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Semaphore;

use crate::WorkloadApi;
use crate::workload::workload_mgr::WorkloadMgr;

#[derive(Debug, Default)]
pub struct PercentNormalizer {
    sum: AtomicUsize,
    count: AtomicUsize,
}

impl PercentNormalizer {
    pub fn update(&self, value: usize) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    pub fn remove(&self, value: usize) {
        self.count.fetch_sub(1, Ordering::Relaxed);
        self.sum.fetch_sub(value, Ordering::Relaxed);
    }

    pub fn get_normalized(&self, value: usize) -> Option<usize> {
        let sum = self.sum.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);

        match sum {
            0 => match count {
                0 => None,
                _ => Some(100 / count),
            },
            _ => Some(value * 100 / sum),
        }
    }
}

struct WorkloadGroupResourceManagerInner {
    workload_mgr: Arc<WorkloadMgr>,
    global_mem_stat: &'static MemStat,
    percent_normalizer: Arc<PercentNormalizer>,
    online_workload_group: Mutex<HashMap<String, Weak<WorkloadGroupResource>>>,
}

pub struct WorkloadGroupResourceManager {
    inner: Arc<WorkloadGroupResourceManagerInner>,
}

impl WorkloadGroupResourceManagerInner {
    pub fn new(
        workload_mgr: Arc<WorkloadMgr>,
        global_mem_stat: &'static MemStat,
    ) -> Arc<WorkloadGroupResourceManagerInner> {
        Arc::new(WorkloadGroupResourceManagerInner {
            workload_mgr,
            global_mem_stat,
            percent_normalizer: Arc::new(Default::default()),
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

    fn remove_workload(&self, id: &str, mem_percentage: Option<usize>) {
        let mut online_workload_group = self
            .online_workload_group
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if online_workload_group.remove(id).is_some()
            && let Some(mem_percentage) = mem_percentage
        {
            self.percent_normalizer.remove(mem_percentage);
            self.update_mem_usage(&online_workload_group);
        }
    }

    pub async fn get_workload(self: &Arc<Self>, id: &str) -> Result<Arc<WorkloadGroupResource>> {
        self.cleanup_expired();

        let workload = self.workload_mgr.get_by_id(id).await?;

        {
            let online_workload_group = self
                .online_workload_group
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(online_workload_group) = online_workload_group.get(id) {
                if let Some(online_workload) = online_workload_group.upgrade() {
                    if online_workload.meta == workload {
                        return Ok(online_workload);
                    }
                }
            }
        }

        let mut online_workload_group = self
            .online_workload_group
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(online_workload_group) = online_workload_group.get(id) {
            if let Some(online_workload) = online_workload_group.upgrade() {
                if online_workload.meta == workload {
                    return Ok(online_workload);
                }
            }
        }

        let mgr_ref = Arc::downgrade(self);
        let queue_key = format!("__fd_workload_queries_queue/queue/{}", workload.id);
        let mut workload_resource = Arc::new(WorkloadGroupResource {
            queue_key,
            meta: workload,
            mem_stat: MemStat::create_workload_group(),
            max_memory_usage: Arc::new(AtomicUsize::new(0)),
            destroy_fn: Some(Box::new(move |id: &str, mem_percentage| {
                if let Some(resource_manager) = mgr_ref.upgrade() {
                    resource_manager.remove_workload(id, mem_percentage);
                }
            })),
            permits: Semaphore::MAX_PERMITS,
            mutex: Arc::new(TokioMutex::new(())),
            semaphore: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        });

        let old_workload_group = online_workload_group.insert(
            workload_resource.meta.id.clone(),
            Arc::downgrade(&workload_resource),
        );

        let Some(old_workload_group) = old_workload_group else {
            let memory_quota = workload_resource.meta.quotas.get(MEMORY_QUOTA_KEY).cloned();
            let concurrency_quota = workload_resource
                .meta
                .quotas
                .get(MAX_CONCURRENCY_QUOTA_KEY)
                .cloned();

            if let Some(QuotaValue::Percentage(v)) = memory_quota {
                self.percent_normalizer.update(v);
            }

            if let Some(QuotaValue::Number(v)) = concurrency_quota {
                let mut_workload_resource =
                    unsafe { Arc::get_mut_unchecked(&mut workload_resource) };
                let permits = std::cmp::min(v, mut_workload_resource.permits);
                let forget_permits = mut_workload_resource.permits - permits;
                mut_workload_resource
                    .semaphore
                    .forget_permits(forget_permits);
                mut_workload_resource.permits = permits;
            }

            self.update_mem_usage(&online_workload_group);

            return Ok(workload_resource);
        };

        if let Some(old_workload_group) = old_workload_group.upgrade() {
            let new_memory_quota = workload_resource.meta.quotas.get(MEMORY_QUOTA_KEY).cloned();
            let old_memory_quota = old_workload_group
                .meta
                .quotas
                .get(MEMORY_QUOTA_KEY)
                .cloned();
            let concurrency_quota = workload_resource
                .meta
                .quotas
                .get(MAX_CONCURRENCY_QUOTA_KEY)
                .cloned();

            let mut_workload_resource = unsafe { Arc::get_mut_unchecked(&mut workload_resource) };
            mut_workload_resource.max_memory_usage = old_workload_group.max_memory_usage.clone();

            match (old_memory_quota, new_memory_quota) {
                (None, Some(QuotaValue::Percentage(v))) => {
                    self.percent_normalizer.update(v);
                }
                (Some(QuotaValue::Percentage(v)), None) => {
                    self.percent_normalizer.remove(v);
                }
                (Some(QuotaValue::Percentage(v)), Some(QuotaValue::Bytes(_))) => {
                    self.percent_normalizer.remove(v);
                }
                (Some(QuotaValue::Percentage(old)), Some(QuotaValue::Percentage(new))) => {
                    self.percent_normalizer.remove(old);
                    self.percent_normalizer.update(new);
                }
                (Some(QuotaValue::Bytes(_)), Some(QuotaValue::Percentage(v))) => {
                    self.percent_normalizer.update(v);
                }
                _ => {}
            }

            self.update_mem_usage(&online_workload_group);

            mut_workload_resource.permits = old_workload_group.permits;
            mut_workload_resource.semaphore = old_workload_group.semaphore.clone();

            match concurrency_quota {
                None => {
                    let old_permit =
                        std::cmp::min(Semaphore::MAX_PERMITS, mut_workload_resource.permits);

                    let add_permit = Semaphore::MAX_PERMITS - old_permit;
                    mut_workload_resource.semaphore.add_permits(add_permit);
                    mut_workload_resource.permits = Semaphore::MAX_PERMITS;
                }
                Some(QuotaValue::Number(v)) if v > mut_workload_resource.permits => {
                    let new_permit = std::cmp::min(v, Semaphore::MAX_PERMITS);
                    let add_permits = new_permit - mut_workload_resource.permits;
                    mut_workload_resource.semaphore.add_permits(add_permits);
                    mut_workload_resource.permits = new_permit;
                }
                Some(QuotaValue::Number(v)) if v < mut_workload_resource.permits => {
                    let new_permit = std::cmp::min(v, Semaphore::MAX_PERMITS);
                    let forget_permits = mut_workload_resource.permits - new_permit;
                    mut_workload_resource
                        .semaphore
                        .forget_permits(forget_permits);
                    mut_workload_resource.permits = new_permit;
                }
                _ => {}
            }
        }

        Ok(workload_resource)
    }

    fn update_mem_usage(&self, workload_groups: &HashMap<String, Weak<WorkloadGroupResource>>) {
        for workload_group_resource in workload_groups.values() {
            if let Some(workload_group) = workload_group_resource.upgrade() {
                if let Some(QuotaValue::Percentage(v)) =
                    workload_group.meta.quotas.get(MEMORY_QUOTA_KEY)
                {
                    if let Some(v) = self.percent_normalizer.get_normalized(*v) {
                        let limit = self.global_mem_stat.get_limit();
                        let usage_ratio = workload_group.meta.get_max_memory_usage_ratio();
                        if limit > 0 {
                            workload_group.max_memory_usage.store(
                                limit as usize / 100 * usage_ratio / 100 * v,
                                Ordering::Relaxed,
                            );
                        }
                    }
                } else if let Some(QuotaValue::Bytes(v)) =
                    workload_group.meta.quotas.get(MEMORY_QUOTA_KEY)
                {
                    let limit = self.global_mem_stat.get_limit();
                    let usage_ratio = workload_group.meta.get_max_memory_usage_ratio();

                    let mut memory_usage = *v;
                    if limit > 0 {
                        let max_memory_usage = limit as usize / 100 * usage_ratio;
                        memory_usage = std::cmp::min(max_memory_usage, memory_usage);
                    }

                    workload_group
                        .max_memory_usage
                        .store(memory_usage, Ordering::Relaxed);
                } else {
                    let limit = self.global_mem_stat.get_limit();
                    let usage_ratio = workload_group.meta.get_max_memory_usage_ratio();
                    let mut memory_usage = 0;

                    if limit > 0 {
                        memory_usage = limit as usize / 100 * usage_ratio;
                    }

                    workload_group
                        .max_memory_usage
                        .store(memory_usage, Ordering::Relaxed)
                }
            }
        }
    }
}

impl WorkloadGroupResourceManager {
    pub fn new(workload_mgr: Arc<WorkloadMgr>) -> Arc<Self> {
        Arc::new(WorkloadGroupResourceManager {
            inner: WorkloadGroupResourceManagerInner::new(workload_mgr, &GLOBAL_MEM_STAT),
        })
    }

    pub async fn get_workload(&self, id: &str) -> Result<Arc<WorkloadGroupResource>> {
        self.inner.get_workload(id).await
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::GLOBAL_QUERIES_MANAGER;
    use databend_common_base::runtime::workload_group::WorkloadGroup;
    use databend_common_meta_store::MetaStore;
    use databend_meta_runtime::DatabendRuntime;

    use super::*;

    async fn create_workload_mgr() -> WorkloadMgr {
        let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
        WorkloadMgr::create(test_api.clone(), "test-tenant-id").unwrap()
    }

    #[tokio::test]
    async fn test_workload_resource() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        let workload = WorkloadGroup {
            id: String::new(),
            name: "test_workload".to_string(),
            quotas: HashMap::new(),
        };

        let workload = workload_mgr.create(workload).await?;

        {
            let workload1 = inner.get_workload(&workload.id).await?;
            assert_eq!(workload1.meta.id, workload.id);
            assert_eq!(workload1.meta.name, "test_workload");

            let workload2 = inner.get_workload(&workload.id).await?;
            assert_eq!(Arc::as_ptr(&workload1), Arc::as_ptr(&workload2));

            assert_eq!(inner.online_workload_group.lock().unwrap().len(), 1);
        }

        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 0);

        Ok(())
    }

    fn create_test_workload(name: &str) -> WorkloadGroup {
        WorkloadGroup {
            id: String::new(),
            name: name.to_string(),
            quotas: HashMap::new(),
        }
    }

    fn create_test_workload_with_mem_quota(name: &str, percentage: usize) -> WorkloadGroup {
        let mut quotas = HashMap::new();
        quotas.insert(
            MEMORY_QUOTA_KEY.to_string(),
            QuotaValue::Percentage(percentage),
        );

        WorkloadGroup {
            id: String::new(),
            name: name.to_string(),
            quotas,
        }
    }

    #[tokio::test]
    async fn test_workload_resource_basic() -> Result<()> {
        static TEST_GLOBAL: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &TEST_GLOBAL);

        let workload = create_test_workload("test_workload");
        let workload = workload_mgr.create(workload).await?;

        {
            // First get should create new resource
            let workload1 = inner.get_workload(&workload.id).await?;
            assert_eq!(workload1.meta.id, workload.id);
            assert_eq!(workload1.meta.name, "test_workload");

            // Second get should return same instance
            let workload2 = inner.get_workload(&workload.id).await?;
            assert_eq!(Arc::as_ptr(&workload1), Arc::as_ptr(&workload2));

            // Should be tracked in online map
            assert_eq!(inner.online_workload_group.lock().unwrap().len(), 1);
        }

        // After dropping all references, should be cleaned up
        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_resource_with_memory_quota() -> Result<()> {
        static LIMIT: i64 = 4 * 1024 * 1024 * 1024;
        static TEST_GLOBAL: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);

        TEST_GLOBAL.set_limit(LIMIT, false);

        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &TEST_GLOBAL);

        let workload = create_test_workload_with_mem_quota("test_workload", 30);
        let workload = workload_mgr.create(workload).await?;

        {
            let workload1 = inner.get_workload(&workload.id).await?;

            // Check memory quota was processed
            assert_eq!(inner.percent_normalizer.count.load(Ordering::Relaxed), 1);
            assert_eq!(inner.percent_normalizer.sum.load(Ordering::Relaxed), 30);

            // Check memory usage was calculated (100% since it's the only workload)
            assert_eq!(
                workload1.max_memory_usage.load(Ordering::Relaxed),
                (LIMIT / 100 * 25 / 100 * 100) as usize
            );
        }

        // After dropping, should be removed from normalizer
        assert_eq!(inner.percent_normalizer.count.load(Ordering::Relaxed), 0);
        assert_eq!(inner.percent_normalizer.sum.load(Ordering::Relaxed), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_workloads_with_memory_quota() -> Result<()> {
        static LIMIT: i64 = 4 * 1024 * 1024 * 1024;
        static TEST_GLOBAL: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);
        TEST_GLOBAL.set_limit(LIMIT, false);

        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &TEST_GLOBAL);

        // Create first workload with 30% quota
        let workload1 = create_test_workload_with_mem_quota("workload1", 30);
        let workload1 = workload_mgr.create(workload1).await?;
        let resource1 = inner.get_workload(&workload1.id).await?;

        // Create second workload with 70% quota
        let workload2 = create_test_workload_with_mem_quota("workload2", 70);
        let workload2 = workload_mgr.create(workload2).await?;
        let resource2 = inner.get_workload(&workload2.id).await?;

        // Check normalizer state
        assert_eq!(inner.percent_normalizer.count.load(Ordering::Relaxed), 2);
        assert_eq!(inner.percent_normalizer.sum.load(Ordering::Relaxed), 100);

        // Check memory allocations are calculated correctly
        assert_eq!(
            resource1.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * 30) as usize
        ); // 30% of total 100
        assert_eq!(
            resource2.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * 70) as usize
        ); // 70% of total 100

        // Drop first workload
        drop(resource1);

        // Check normalizer updated and second workload's allocation recalculated
        assert_eq!(inner.percent_normalizer.count.load(Ordering::Relaxed), 1);
        assert_eq!(inner.percent_normalizer.sum.load(Ordering::Relaxed), 70);
        assert_eq!(
            resource2.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * 100) as usize
        ); // Now 100% of remaining 70

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_quota_update() -> Result<()> {
        static LIMIT: i64 = 4 * 1024 * 1024 * 1024;
        static TEST_GLOBAL: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);
        TEST_GLOBAL.set_limit(LIMIT, false);

        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &TEST_GLOBAL);

        // Create initial workload with 50% quota
        let workload1 = create_test_workload_with_mem_quota("workload", 50);
        let workload1 = workload_mgr.create(workload1).await?;
        let resource1 = inner.get_workload(&workload1.id).await?;

        let workload2 = create_test_workload_with_mem_quota("workload2", 50);
        let workload2 = workload_mgr.create(workload2).await?;
        let resource2 = inner.get_workload(&workload2.id).await?;

        assert_eq!(
            resource1.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * 50) as usize
        );
        assert_eq!(
            resource2.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * 50) as usize
        );

        workload_mgr
            .set_quotas(
                "workload".to_string(),
                HashMap::from([(MEMORY_QUOTA_KEY.to_string(), QuotaValue::Percentage(70))]),
            )
            .await?;

        let updated_resource = inner.get_workload(&workload1.id).await?;

        // Should be same resource instance
        assert_ne!(Arc::as_ptr(&resource1), Arc::as_ptr(&updated_resource));

        // Check normalizer updated
        assert_eq!(inner.percent_normalizer.count.load(Ordering::Relaxed), 2);
        assert_eq!(
            inner.percent_normalizer.sum.load(Ordering::Relaxed),
            50 + 70
        );

        // Memory usage should be recalculated
        assert_eq!(
            resource1.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * (70 * 100 / (70 + 50))) as usize
        );
        assert_eq!(
            resource2.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * (50 * 100 / (70 + 50))) as usize
        );
        assert_eq!(
            updated_resource.max_memory_usage.load(Ordering::Relaxed),
            (LIMIT / 100 * 25 / 100 * (70 * 100 / (70 + 50))) as usize
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_cleanup_expired() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Create and get workload
        let workload = create_test_workload("workload");
        let workload = workload_mgr.create(workload).await?;
        let _resource = inner.get_workload(&workload.id).await?;

        // Should be in online map
        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 1);

        // Explicitly cleanup
        inner.cleanup_expired();

        // Should still be there since we hold a reference
        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 1);

        // Drop reference and cleanup
        drop(_resource);
        inner.cleanup_expired();

        // Should be removed
        assert_eq!(inner.online_workload_group.lock().unwrap().len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_quota_basic() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Create workload with concurrency quota
        let mut quotas = HashMap::new();
        quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(5));

        let workload = WorkloadGroup {
            id: String::new(),
            name: "concurrency_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource = inner.get_workload(&created_workload.id).await?;

        // Verify concurrency control is set up correctly
        assert_eq!(resource.permits, 5);
        assert_eq!(resource.semaphore.available_permits(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_quota_acquisition() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Create workload with limited concurrency
        let mut quotas = HashMap::new();
        quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(2));

        let workload = WorkloadGroup {
            id: String::new(),
            name: "concurrency_limit_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource = inner.get_workload(&created_workload.id).await?;

        // Acquire permits up to limit
        let permit1 = resource.semaphore.clone().try_acquire_owned();
        let permit2 = resource.semaphore.clone().try_acquire_owned();
        let permit3 = resource.semaphore.clone().try_acquire_owned();

        assert!(
            permit1.is_ok(),
            "First permit should be acquired successfully"
        );
        assert!(
            permit2.is_ok(),
            "Second permit should be acquired successfully"
        );
        assert!(permit3.is_err(), "Third permit should fail due to limit");
        assert_eq!(resource.semaphore.available_permits(), 0);

        // Release one permit and try again
        drop(permit1);
        let permit4 = resource.semaphore.clone().try_acquire_owned();
        assert!(permit4.is_ok(), "Should be able to acquire after release");

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_quota_update() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Start with concurrency limit of 3
        let mut quotas = HashMap::new();
        quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(3));

        let workload = WorkloadGroup {
            id: String::new(),
            name: "concurrency_update_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource1 = inner.get_workload(&created_workload.id).await?;
        assert_eq!(resource1.permits, 3);
        assert_eq!(resource1.semaphore.available_permits(), 3);

        // Update to increase concurrency limit to 5
        let mut new_quotas = HashMap::new();
        new_quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(5));

        workload_mgr
            .set_quotas("concurrency_update_test".to_string(), new_quotas)
            .await?;
        let resource2 = inner.get_workload(&created_workload.id).await?;

        assert_eq!(resource2.permits, 5);
        assert_eq!(resource2.semaphore.available_permits(), 5);

        // Update to decrease concurrency limit to 2
        let mut decreased_quotas = HashMap::new();
        decreased_quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(2));

        workload_mgr
            .set_quotas("concurrency_update_test".to_string(), decreased_quotas)
            .await?;
        let resource3 = inner.get_workload(&created_workload.id).await?;

        assert_eq!(resource3.permits, 2);
        assert_eq!(resource3.semaphore.available_permits(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_quota_removal() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Start with concurrency limit
        let mut quotas = HashMap::new();
        quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(3));

        let workload = WorkloadGroup {
            id: String::new(),
            name: "concurrency_removal_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource1 = inner.get_workload(&created_workload.id).await?;
        assert_eq!(resource1.permits, 3);

        // Remove concurrency quota (should revert to unlimited)
        workload_mgr
            .unset_quotas("concurrency_removal_test".to_string(), vec![
                MAX_CONCURRENCY_QUOTA_KEY.to_string(),
            ])
            .await?;
        let resource2 = inner.get_workload(&created_workload.id).await?;

        use tokio::sync::Semaphore;
        assert_eq!(resource2.permits, Semaphore::MAX_PERMITS);
        assert_eq!(
            resource2.semaphore.available_permits(),
            Semaphore::MAX_PERMITS
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_with_max_permits_boundary() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Test with value larger than MAX_PERMITS
        let mut quotas = HashMap::new();
        quotas.insert(
            MAX_CONCURRENCY_QUOTA_KEY.to_string(),
            QuotaValue::Number(usize::MAX),
        );

        let workload = WorkloadGroup {
            id: String::new(),
            name: "max_permits_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource = inner.get_workload(&created_workload.id).await?;

        // Should be clamped to MAX_PERMITS
        use tokio::sync::Semaphore;
        assert_eq!(resource.permits, Semaphore::MAX_PERMITS);
        assert_eq!(
            resource.semaphore.available_permits(),
            Semaphore::MAX_PERMITS
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_workload_concurrency_zero_permits() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Test with zero concurrency (should block all)
        let mut quotas = HashMap::new();
        quotas.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(0));

        let workload = WorkloadGroup {
            id: String::new(),
            name: "zero_permits_test".to_string(),
            quotas,
        };

        let created_workload = workload_mgr.create(workload).await?;
        let resource = inner.get_workload(&created_workload.id).await?;

        assert_eq!(resource.permits, 0);
        assert_eq!(resource.semaphore.available_permits(), 0);

        // Should not be able to acquire any permits
        let permit_result = resource.semaphore.clone().try_acquire_owned();
        assert!(
            permit_result.is_err(),
            "Should not be able to acquire with zero permits"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_workloads_concurrency_isolation() -> Result<()> {
        let workload_mgr = Arc::new(create_workload_mgr().await);
        let inner = WorkloadGroupResourceManagerInner::new(workload_mgr.clone(), &GLOBAL_MEM_STAT);

        // Create workload 1 with limit 2
        let mut quotas1 = HashMap::new();
        quotas1.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(2));
        let workload1 = WorkloadGroup {
            id: String::new(),
            name: "workload_1".to_string(),
            quotas: quotas1,
        };
        let created1 = workload_mgr.create(workload1).await?;
        let resource1 = inner.get_workload(&created1.id).await?;

        // Create workload 2 with limit 3
        let mut quotas2 = HashMap::new();
        quotas2.insert(MAX_CONCURRENCY_QUOTA_KEY.to_string(), QuotaValue::Number(3));
        let workload2 = WorkloadGroup {
            id: String::new(),
            name: "workload_2".to_string(),
            quotas: quotas2,
        };
        let created2 = workload_mgr.create(workload2).await?;
        let resource2 = inner.get_workload(&created2.id).await?;

        // Verify isolation - each workload has independent limits
        assert_eq!(resource1.permits, 2);
        assert_eq!(resource2.permits, 3);

        // Exhaust permits in workload 1
        let permit1_1 = resource1.semaphore.clone().try_acquire_owned().unwrap();
        let permit1_2 = resource1.semaphore.clone().try_acquire_owned().unwrap();
        let permit1_3 = resource1.semaphore.clone().try_acquire_owned();
        assert!(permit1_3.is_err(), "Workload 1 should be exhausted");

        // Workload 2 should still have permits available
        let permit2_1 = resource2.semaphore.clone().try_acquire_owned().unwrap();
        let permit2_2 = resource2.semaphore.clone().try_acquire_owned().unwrap();
        let permit2_3 = resource2.semaphore.clone().try_acquire_owned().unwrap();
        assert_eq!(resource2.semaphore.available_permits(), 0);

        // Cleanup
        drop(permit1_1);
        drop(permit1_2);
        drop(permit2_1);
        drop(permit2_2);
        drop(permit2_3);

        Ok(())
    }
}

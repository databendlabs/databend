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

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(capacity: usize) -> Arc<PythonWorkerManager> {
        PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: capacity,
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn reserves_minimum_query_budget_and_releases_on_drop() {
        let manager = manager(2);
        let first = manager.query("a", 1);
        first.register().unwrap();
        first.register().unwrap();
        assert!(first.register().is_err());
        first.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 0);
        assert_eq!(first.parallelism, 1);
        assert!(first.try_extra().is_none());
        assert!(first.register().is_err());
        let second = manager.query("b", 1);
        second.register().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.admit())
                .await
                .is_err()
        );
        assert_eq!(second.tenant_slots.available_permits(), 2);
        drop(first);
        second.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 1);
        drop(second);
        assert_eq!(manager.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn tenant_quota_does_not_block_other_tenants() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 2,
            max_workers_per_tenant: 1,
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a", 8);
        let second = manager.query("a", 8);
        let other = manager.query("b", 8);
        for query in [&first, &second, &other] {
            query.register().unwrap();
        }
        first.admit().await.unwrap();
        assert_eq!(first.parallelism, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.admit())
                .await
                .is_err()
        );
        other.admit().await.unwrap();
        assert_eq!(manager.slots.available_permits(), 0);
    }

    #[tokio::test]
    async fn fragment_completion_does_not_finish_other_pipelines() {
        let manager = manager(16);
        let query = manager.query("a", 8);
        let first = query.pipeline();
        let second = query.pipeline();
        query.register().unwrap();
        query.admit().await.unwrap();
        drop(first);
        assert!(!query.is_finished());
        assert_eq!(manager.slots.available_permits(), 15);
        drop(second);
        assert!(query.is_finished());
        assert_eq!(manager.slots.available_permits(), 16);
    }

    #[tokio::test]
    async fn finish_keeps_quota_until_worker_reservations_are_dropped() {
        let manager = manager(16);
        let query = manager.query("a", 8);
        query.register().unwrap();
        let minimum = query.admit().await.unwrap();
        let extra = query.try_extra().unwrap();
        query.finish();
        assert!(query.admit().await.is_err());
        assert!(query.register().is_err());
        assert_eq!(manager.slots.available_permits(), 14);
        drop(minimum);
        assert_eq!(manager.slots.available_permits(), 15);
        drop(extra);
        assert_eq!(manager.slots.available_permits(), 16);
    }

    #[tokio::test]
    async fn admission_queue_is_bounded_and_times_out() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 1,
            max_queued_queries: 1,
            queue_timeout: Duration::from_millis(20),
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a", 8);
        first.register().unwrap();
        first.admit().await.unwrap();
        let second = manager.query("a", 8);
        second.register().unwrap();
        let third = manager.query("b", 8);
        third.register().unwrap();
        let waiting = second.admit();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(
            third
                .admit()
                .await
                .unwrap_err()
                .message()
                .contains("queue is full")
        );
        assert!(waiting.await.unwrap_err().message().contains("timed out"));
        first.finish();
        third.admit().await.unwrap();
    }

    #[test]
    fn reclamation_registry_only_tracks_live_python_queries() {
        let manager = manager(8);
        let _ordinary = manager.query("a", 4);
        assert!(manager.queries.lock().is_empty());
        let finished = manager.query("a", 4);
        finished.register().unwrap();
        finished.register().unwrap();
        assert_eq!(manager.queries.lock().len(), 1);
        finished.finish();
        let current = manager.query("a", 4);
        current.register().unwrap();
        assert_eq!(manager.queries.lock().len(), 1);
    }

    #[tokio::test]
    async fn high_core_defaults_follow_max_threads_without_preallocating() {
        for cores in [64, 128] {
            let config = PythonWorkerConfig::default().resolve_limits(cores).unwrap();
            assert_eq!(config.max_workers, cores);
            assert_eq!(config.max_workers_per_tenant, cores);
            assert_eq!(config.max_workers_per_query, 0);
            let manager = PythonWorkerManager::new(config).unwrap();
            let query = manager.query("a", cores);
            assert_eq!(query.parallelism, cores);
            query.register().unwrap();
            query.admit().await.unwrap();
            // One small query does not reserve the whole machine's CPU budget.
            assert_eq!(manager.slots.available_permits(), cores - 1);
            let extras: Vec<_> = (1..cores).map(|_| query.try_extra().unwrap()).collect();
            assert!(query.try_extra().is_none());
            assert_eq!(manager.slots.available_permits(), 0);
            query.finish();
            assert_eq!(manager.slots.available_permits(), 1);
            drop(extras);
            assert_eq!(manager.slots.available_permits(), cores);
        }
    }

    #[tokio::test]
    async fn multiple_operators_do_not_multiply_execution_parallelism() {
        let manager = manager(8);
        let query = manager.query("a", 4);
        query.register().unwrap();
        query.register().unwrap();
        query.admit().await.unwrap();
        assert_eq!(query.execution_slots.available_permits(), 4);
        let extras: Vec<_> = (0..3).map(|_| query.try_extra().unwrap()).collect();
        assert!(query.try_extra().is_none());
        // Two minimum workers plus at most max_threads-1 extras; not 2*4.
        assert_eq!(manager.slots.available_permits(), 3);
        query.finish();
        drop(extras);
        assert_eq!(manager.slots.available_permits(), 8);
    }

    #[tokio::test]
    async fn explicit_query_limit_caps_resident_workers() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 128,
            max_workers_per_query: 3,
            ..Default::default()
        })
        .unwrap();
        let query = manager.query("a", 128);
        assert_eq!(query.parallelism, 3);
        query.register().unwrap();
        query.register().unwrap();
        query.admit().await.unwrap();
        let extra = query.try_extra().unwrap();
        assert!(query.try_extra().is_none());
        assert_eq!(manager.slots.available_permits(), 125);
        query.finish();
        drop(extra);
        assert_eq!(manager.slots.available_permits(), 128);
    }

    #[tokio::test]
    async fn extra_growth_yields_to_waiting_minimum_admission() {
        let manager = manager(2);
        let first = manager.query("a", 2);
        first.register().unwrap();
        first.admit().await.unwrap();
        let second = manager.query("a", 2);
        second.register().unwrap();
        second.register().unwrap();
        let waiting = second.admit();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(first.try_extra().is_none());
        first.finish();
        waiting.await.unwrap();
        assert_eq!(manager.slots.available_permits(), 0);
    }

    #[tokio::test]
    async fn cancelled_minimum_admission_wakes_pool_growth() {
        let manager = PythonWorkerManager::new(PythonWorkerConfig {
            max_workers: 3,
            max_workers_per_tenant: 2,
            ..Default::default()
        })
        .unwrap();
        let first = manager.query("a", 2);
        first.register().unwrap();
        first.admit().await.unwrap();
        let second = manager.query("a", 2);
        second.register().unwrap();
        second.register().unwrap();
        let mut waiting = Box::pin(second.admit());
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(first.try_extra().is_none());
        let changed = manager.capacity_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        drop(waiting);
        tokio::time::timeout(Duration::from_millis(100), changed)
            .await
            .unwrap();
        assert!(first.try_extra().is_some());
        first.finish();
        second.admit().await.unwrap();
    }

    #[tokio::test]
    async fn release_notification_observes_already_released_permits() {
        let manager = manager(2);
        let query = manager.query("a", 2);
        query.register().unwrap();
        query.admit().await.unwrap();
        let extra = query.try_extra().unwrap();
        let changed = manager.capacity_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        drop(extra);
        changed.await;
        assert!(query.try_extra().is_some());
    }

    #[test]
    fn arrow_ipc_preserves_zero_argument_row_count_and_nulls() {
        use arrow_array::Int64Array;
        use arrow_array::RecordBatchOptions;
        use arrow_schema::DataType;
        use arrow_schema::Schema;
        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(
            decode_batch(&encode_batch(&empty).unwrap())
                .unwrap()
                .num_rows(),
            3
        );
        let field = Field::new("x", DataType::Int64, true).with_metadata(HashMap::from([(
            "extension-key".to_owned(),
            "extension-value".to_owned(),
        )]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(
            Int64Array::from(vec![Some(42), None, Some(-1)]),
        )])
        .unwrap();
        assert_eq!(decode_batch(&encode_batch(&batch).unwrap()).unwrap(), batch);
    }

    #[test]
    fn validates_config_and_frame_size() {
        assert!(
            PythonWorkerManager::new(PythonWorkerConfig {
                max_workers: 4,
                max_workers_per_tenant: 8,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            PythonWorkerManager::new(PythonWorkerConfig {
                max_workers: 16,
                max_workers_per_query: 17,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            PythonWorkerManager::new(PythonWorkerConfig {
                max_queued_queries: 0,
                ..Default::default()
            })
            .is_err()
        );
        assert!(check_length(MAX_FRAME_BYTES + 1).is_err());
    }
}

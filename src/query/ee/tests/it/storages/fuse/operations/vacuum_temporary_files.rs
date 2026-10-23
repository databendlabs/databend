// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod unit {
    #![allow(dead_code)]

    include!("../../../../../src/storages/fuse/operations/vacuum_temporary_files.rs");

    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use chrono::Duration as ChronoDuration;
    use chrono::Utc;
    use databend_common_catalog::table_context::CheckAbort;
    use databend_common_storage::DatabendRetryInterceptor;
    use opendal::EntryMode;
    use opendal::Metadata;
    use opendal::OperatorBuilder;
    use opendal::layers::RetryLayer;
    use opendal::raw::Access;
    use opendal::raw::AccessorInfo;
    use opendal::raw::MaybeSend;
    use opendal::raw::OpDelete;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpStat;
    use opendal::raw::OpWrite;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpStat;
    use opendal::raw::RpWrite;
    use opendal::raw::Timestamp;
    use opendal::raw::oio;
    use opendal::raw::oio::Entry;
    use parking_lot::Mutex;

    #[derive(Clone, Debug)]
    enum StatResult {
        Meta(Box<Metadata>),
        NotFound,
    }

    #[derive(Clone, Debug)]
    enum ReadResult {
        Buffer(Buffer),
        NotFound,
    }

    #[derive(Debug)]
    struct VecLister(Vec<(String, Metadata)>);

    impl oio::List for VecLister {
        fn next(&mut self) -> impl Future<Output = opendal::Result<Option<Entry>>> + MaybeSend {
            let me = &mut self.0;
            async move { Ok(me.pop().map(|(path, metadata)| Entry::new(&path, metadata))) }
        }
    }

    #[derive(Debug)]
    struct RecordingDeleter {
        deleted: Arc<Mutex<Vec<String>>>,
        pending: Vec<String>,
        /// Flushes that still succeed before `failing_batches` kicks in.
        passing_batches: Arc<AtomicUsize>,
        failing_batches: Arc<AtomicUsize>,
        partial_successes: Arc<AtomicUsize>,
        failure_kind: opendal::ErrorKind,
    }

    impl oio::Delete for RecordingDeleter {
        fn delete(&mut self, path: &str, _args: OpDelete) -> opendal::Result<()> {
            self.pending.push(path.to_string());
            Ok(())
        }

        async fn flush(&mut self) -> opendal::Result<usize> {
            let passing = self
                .passing_batches
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok();
            let failing = !passing
                && self
                    .failing_batches
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                    .is_ok();
            if failing {
                // Some deletes may have reached the backend even though the whole request
                // is unacknowledged. Keep every path pending so a retry is idempotent.
                let n = self
                    .partial_successes
                    .swap(0, Ordering::AcqRel)
                    .min(self.pending.len());
                self.deleted
                    .lock()
                    .extend(self.pending[..n].iter().cloned());
                // A `RetryLayer`, if present, retries this temporary failure.
                return Err(
                    opendal::Error::new(self.failure_kind, "mock batch failure").set_temporary()
                );
            }
            let n = self.pending.len();
            self.deleted.lock().append(&mut self.pending);
            Ok(n)
        }
    }

    /// `(path, content)` pairs written back by the vacuum, in order.
    type Writes = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

    /// Records what the vacuum writes back (manifest rewrites) under the path it was opened for.
    #[derive(Debug)]
    struct RecordingWriter {
        path: String,
        buf: Vec<u8>,
        written: Writes,
    }

    impl oio::Write for RecordingWriter {
        async fn write(&mut self, bs: Buffer) -> opendal::Result<()> {
            self.buf.extend_from_slice(&bs.to_bytes());
            Ok(())
        }

        async fn close(&mut self) -> opendal::Result<Metadata> {
            self.written
                .lock()
                .push((self.path.clone(), std::mem::take(&mut self.buf)));
            Ok(Metadata::new(EntryMode::FILE))
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockVacuumAccessor {
        list_entries: HashMap<String, Vec<(String, Metadata)>>,
        stat_results: HashMap<String, StatResult>,
        read_results: HashMap<String, ReadResult>,
        deleted: Arc<Mutex<Vec<String>>>,
        written: Writes,
        passing_batches: Arc<AtomicUsize>,
        failing_batches: Arc<AtomicUsize>,
        /// Unacknowledged deletes performed by the next failing request.
        partial_successes: Arc<AtomicUsize>,
        failure_kind: Mutex<opendal::ErrorKind>,
        list_calls: AtomicUsize,
        read_calls: AtomicUsize,
        delete_calls: AtomicUsize,
        /// Paths per delete request the mock backend "accepts".
        delete_max_size: AtomicUsize,
    }

    impl MockVacuumAccessor {
        fn new(
            list_entries: HashMap<String, Vec<(String, Metadata)>>,
            stat_results: HashMap<String, StatResult>,
            read_results: HashMap<String, ReadResult>,
        ) -> Arc<Self> {
            Arc::new(Self {
                list_entries,
                stat_results,
                read_results,
                deleted: Arc::new(Mutex::new(Vec::new())),
                written: Arc::new(Mutex::new(Vec::new())),
                passing_batches: Arc::new(AtomicUsize::new(0)),
                failing_batches: Arc::new(AtomicUsize::new(0)),
                partial_successes: Arc::new(AtomicUsize::new(0)),
                failure_kind: Mutex::new(opendal::ErrorKind::RateLimited),
                list_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
                delete_calls: AtomicUsize::new(0),
                delete_max_size: AtomicUsize::new(1000),
            })
        }

        /// Set before building the `SpillDeleter`, which reads the capability once.
        fn set_delete_max_size(&self, n: usize) {
            self.delete_max_size.store(n, Ordering::Release);
        }

        fn throttle_next_batches(&self, n: usize) {
            self.fail_next_batches(n, opendal::ErrorKind::RateLimited);
        }

        fn fail_next_batches(&self, n: usize, kind: opendal::ErrorKind) {
            self.fail_batches_after(0, n, kind);
        }

        /// Let `ok` delete requests through, then fail the next `n` with `kind`.
        fn fail_batches_after(&self, ok: usize, n: usize, kind: opendal::ErrorKind) {
            *self.failure_kind.lock() = kind;
            self.passing_batches.store(ok, Ordering::Release);
            self.failing_batches.store(n, Ordering::Release);
        }

        fn partially_fail_batch_after(&self, ok: usize, removed: usize, kind: opendal::ErrorKind) {
            self.fail_batches_after(ok, 1, kind);
            self.partial_successes.store(removed, Ordering::Release);
        }

        fn deleted_paths(&self) -> Vec<String> {
            self.deleted.lock().clone()
        }

        /// Every `(path, content)` written back, in order.
        fn written(&self) -> Vec<(String, String)> {
            self.written
                .lock()
                .iter()
                .map(|(p, b)| (p.clone(), String::from_utf8_lossy(b).into_owned()))
                .collect()
        }

        fn list_call_count(&self) -> usize {
            self.list_calls.load(Ordering::Acquire)
        }

        fn read_call_count(&self) -> usize {
            self.read_calls.load(Ordering::Acquire)
        }

        /// One per delete request sent to the backend, i.e. per `SpillDeleter::delete` while
        /// the mock accepts at least as many paths per request as the vacuum batches.
        fn delete_call_count(&self) -> usize {
            self.delete_calls.load(Ordering::Acquire)
        }
    }

    impl Access for MockVacuumAccessor {
        type Reader = Buffer;
        type Writer = RecordingWriter;
        type Lister = VecLister;
        type Deleter = RecordingDeleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                stat: true,
                read: true,
                write: true,
                list: true,
                delete: true,
                delete_max_size: Some(self.delete_max_size.load(Ordering::Acquire)),
                ..Default::default()
            });
            info.into()
        }

        async fn stat(&self, path: &str, _args: OpStat) -> opendal::Result<RpStat> {
            match self
                .stat_results
                .get(path)
                .cloned()
                .unwrap_or(StatResult::NotFound)
            {
                StatResult::Meta(meta) => Ok(RpStat::new(*meta)),
                StatResult::NotFound => Err(opendal::Error::new(
                    opendal::ErrorKind::NotFound,
                    "mock stat not found",
                )),
            }
        }

        async fn read(&self, path: &str, _args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            self.read_calls.fetch_add(1, Ordering::AcqRel);
            match self
                .read_results
                .get(path)
                .cloned()
                .unwrap_or(ReadResult::NotFound)
            {
                ReadResult::Buffer(buf) => Ok((RpRead::new(), buf)),
                ReadResult::NotFound => Err(opendal::Error::new(
                    opendal::ErrorKind::NotFound,
                    "mock read not found",
                )),
            }
        }

        async fn write(
            &self,
            path: &str,
            _args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            Ok((RpWrite::default(), RecordingWriter {
                path: path.to_string(),
                buf: Vec::new(),
                written: self.written.clone(),
            }))
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.delete_calls.fetch_add(1, Ordering::AcqRel);
            Ok((RpDelete::default(), RecordingDeleter {
                deleted: self.deleted.clone(),
                pending: Vec::new(),
                passing_batches: self.passing_batches.clone(),
                failing_batches: self.failing_batches.clone(),
                partial_successes: self.partial_successes.clone(),
                failure_kind: *self.failure_kind.lock(),
            }))
        }

        async fn list(&self, path: &str, _args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.list_calls.fetch_add(1, Ordering::AcqRel);
            let mut entries = self.list_entries.get(path).cloned().unwrap_or_default();
            entries.reverse();
            Ok((RpList::default(), VecLister(entries)))
        }
    }

    fn file_meta(last_modified_millis: Option<i64>) -> Metadata {
        let mut meta = Metadata::new(EntryMode::FILE).with_content_length(0);
        if let Some(last_modified_millis) = last_modified_millis {
            meta =
                meta.with_last_modified(Timestamp::from_millisecond(last_modified_millis).unwrap());
        }
        meta
    }

    fn dir_meta(last_modified_millis: Option<i64>) -> Metadata {
        let mut meta = Metadata::new(EntryMode::DIR);
        if let Some(last_modified_millis) = last_modified_millis {
            meta =
                meta.with_last_modified(Timestamp::from_millisecond(last_modified_millis).unwrap());
        }
        meta
    }

    fn expiry(now_ms: i64, retain: Duration) -> Expiry {
        Expiry {
            now_ms,
            retain_ms: retain.as_millis() as i64,
        }
    }

    struct NoAbort;

    impl CheckAbort for NoAbort {
        fn try_check_aborting(&self) -> Result<()> {
            Ok(())
        }
    }

    /// Aborts from the `n`-th check on.
    struct AbortAfter(AtomicUsize);

    impl AbortAfter {
        fn checks(n: usize) -> Arc<Self> {
            Arc::new(Self(AtomicUsize::new(n)))
        }
    }

    impl CheckAbort for AbortAfter {
        fn try_check_aborting(&self) -> Result<()> {
            if self
                .0
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok()
            {
                Ok(())
            } else {
                Err(ErrorCode::AbortedQuery("aborted by test"))
            }
        }
    }

    fn scheduled() -> Pacing {
        Pacing::Scheduled(Arc::new(NoAbort))
    }

    /// Drive the real per-directory unit of work and return its outcome.
    async fn run_dir_vacuum_for_test(
        operator: &Operator,
        temporary_dir: &str,
        dir_path: &str,
        metadata: &Metadata,
        expiry: Expiry,
        max: usize,
    ) -> Result<DirOutcome> {
        vacuum_dir_entry(
            Arc::new(SpillDeleter::new(operator.clone(), Arc::default())),
            temporary_dir.into(),
            dir_path.to_owned(),
            metadata.clone(),
            expiry,
            max,
            scheduled(),
        )
        .await
    }

    fn clear() -> DeleteOutcome {
        DeleteOutcome {
            removed: 1,
            charged: 1,
            failed: false,
            rate_limited: false,
            retried: 0,
        }
    }

    fn rate_limited() -> DeleteOutcome {
        DeleteOutcome {
            removed: 0,
            charged: 1,
            failed: true,
            rate_limited: true,
            retried: 0,
        }
    }

    /// Rejected for a reason other than throttling (network, backend error).
    fn failed() -> DeleteOutcome {
        DeleteOutcome {
            removed: 0,
            charged: 1,
            failed: true,
            rate_limited: false,
            retried: 0,
        }
    }

    fn retried(n: usize) -> DeleteOutcome {
        DeleteOutcome {
            removed: 1,
            charged: 1,
            failed: false,
            rate_limited: false,
            retried: n,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_shared_deleter_observes_requests_not_directory_outcomes() {
        let accessor = MockVacuumAccessor::new(HashMap::new(), HashMap::new(), HashMap::new());
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );
        let initial = deleter.deleter.concurrency();
        let mut run = Run::new(usize::MAX, deleter.deleter.clone());
        // Sixteen successful requests raise the shared bound once. Settling their directory
        // outcomes only accounts for the budget, not a second concurrency feedback signal.
        for i in 0..16 {
            let delete = deleter
                .delete(vec![format!("spill/dir/file{i}")], "spill/dir/")
                .await;
            run.settle(DirOutcome {
                dir_path: "spill/dir/".to_owned(),
                delete: Some(delete),
            });
        }
        let raised = initial + 1;
        assert_eq!(deleter.deleter.concurrency(), raised);
        assert_eq!(run.budget.removed(), 16);
        assert_eq!(run.budget.charged(), 16);

        // An exhausted rate-limited request lowers the bound before a directory is settled.
        accessor.throttle_next_batches(1);
        let delete = deleter
            .delete(vec!["spill/dir/failed".to_owned()], "spill/dir/")
            .await;
        assert!(delete.failed && delete.rate_limited);
        assert_eq!(deleter.deleter.concurrency(), raised / 2);
        run.settle(DirOutcome {
            dir_path: "spill/dir/".to_owned(),
            delete: Some(delete),
        });
        assert_eq!(deleter.deleter.concurrency(), raised / 2);
        assert_eq!(run.budget.removed(), 16);
        assert_eq!(run.budget.charged(), 17);
    }

    #[test]
    fn test_pacing_yields_only_under_the_scheduler() {
        let scheduled = scheduled();
        assert!(scheduled.continues_after(&clear()));
        assert!(!scheduled.continues_after(&retried(1)));
        assert!(!scheduled.continues_after(&rate_limited()));

        // The query hook has no scheduler to hand the signal to and no later pass for its
        // query: absorbed retries must not strand the rest of the manifest.
        assert!(Pacing::Drain.continues_after(&clear()));
        assert!(Pacing::Drain.continues_after(&retried(1)));
        assert!(
            !Pacing::Drain.continues_after(&failed()),
            "a failed batch still stops"
        );
        assert!(!Pacing::Drain.continues_after(&rate_limited()));

        // The command stops issuing batches once it is aborted.
        let aborted = Pacing::Scheduled(AbortAfter::checks(0));
        assert!(!aborted.continues_after(&clear()));
    }

    #[test]
    fn test_run_budget_charges_failed_requests_but_reports_removed() {
        let mut budget = RunBudget::new(10);
        let mut three = clear();
        three.removed = 3;
        three.charged = 3;
        budget.record(&three);
        budget.record(&three);
        assert_eq!(budget.removed(), 6);
        assert_eq!(budget.remaining(), 4);
        assert!(!budget.exhausted());

        // A request that failed half-way may have removed some of its objects: `LIMIT` is
        // charged for all of them, the command only reports what is known to be gone.
        let mut mixed = failed();
        mixed.removed = 2;
        mixed.charged = 4;
        budget.record(&mixed);
        assert_eq!(budget.removed(), 8);
        assert!(budget.exhausted());
        assert_eq!(budget.remaining(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_failed_batch_is_charged_but_not_counted() -> anyhow::Result<()> {
        let accessor = MockVacuumAccessor::new(HashMap::new(), HashMap::new(), HashMap::new());
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );
        let paths = vec!["spill/dir/file1".to_string(), "spill/dir/file2".to_string()];

        // A failed request may have removed part of its objects before the error, a
        // rate-limited one included: nothing is counted, everything is charged.
        accessor.throttle_next_batches(1);
        let outcome = deleter.delete(paths.clone(), "spill/dir/").await;
        assert_eq!(outcome.removed, 0);
        assert_eq!(outcome.charged, 2);
        assert!(outcome.rate_limited);
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());

        accessor.fail_next_batches(1, opendal::ErrorKind::Unexpected);
        let outcome = deleter.delete(paths.clone(), "spill/dir/").await;
        assert_eq!(outcome.removed, 0);
        assert_eq!(outcome.charged, 2);
        assert!(outcome.failed && !outcome.rate_limited);

        let outcome = deleter.delete(paths.clone(), "spill/dir/").await;
        assert_eq!(outcome.removed, 2);
        assert_eq!(outcome.charged, 2);
        assert!(!outcome.rate_limited);
        assert_eq!(accessor.deleted_paths(), paths);
        Ok(())
    }

    /// OSS, COS, OBS and Azure answer throttling with a `503` that OpenDAL classifies as a
    /// temporary `Unexpected` error, not `RateLimited`. Hidden by the retry layer, it must still
    /// reach the outcome as a retry so the concurrency policy can back off on those stores.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_retried_batch_is_observed_whatever_the_error_kind() -> anyhow::Result<()> {
        for kind in [
            opendal::ErrorKind::Unexpected,
            opendal::ErrorKind::RateLimited,
        ] {
            let accessor = MockVacuumAccessor::new(HashMap::new(), HashMap::new(), HashMap::new());
            let retry_stats = Arc::new(RetryStats::default());
            let operator = OperatorBuilder::new(accessor.clone())
                .layer(
                    RetryLayer::new()
                        .with_min_delay(Duration::ZERO)
                        .with_max_delay(Duration::ZERO)
                        .with_max_times(3)
                        .with_notify(DatabendRetryInterceptor::new(Some(retry_stats.clone()))),
                )
                .finish();
            let deleter = SpillDeleter::new(operator, retry_stats);
            let paths = vec!["spill/dir/file1".to_string(), "spill/dir/file2".to_string()];

            accessor.fail_next_batches(1, kind);
            let outcome = deleter.delete(paths.clone(), "spill/dir/").await;

            assert!(
                !outcome.failed,
                "{kind:?}: the retry must have gone through"
            );
            assert_eq!(outcome.removed, 2);
            assert_eq!(outcome.retried, 1, "{kind:?}");
            assert!(
                !outcome.rate_limited,
                "{kind:?}: only an exhausted batch is"
            );
            assert_eq!(accessor.deleted_paths(), paths);
            assert!(
                !scheduled().continues_after(&outcome),
                "{kind:?}: a retried batch ends a scheduled task"
            );
        }
        Ok(())
    }

    /// The shared deleter observes the retry, while the spill caller still decides whether
    /// to stop between manifest chunks: scheduled cleanup defers, query-finish cleanup drains.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_hidden_retry_preserves_scheduled_and_query_hook_pacing() -> anyhow::Result<()> {
        for kind in [
            opendal::ErrorKind::Unexpected,
            opendal::ErrorKind::RateLimited,
        ] {
            for query_hook in [false, true] {
                let mut lines: Vec<_> = (0..DELETE_BATCH_FILES + 5)
                    .map(|i| format!("spill/dir/file{i}"))
                    .collect();
                lines.extend(["spill/dir/".to_owned(), "spill/dir.list".to_owned()]);
                let accessor = MockVacuumAccessor::new(
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::from([(
                        "spill/dir.list".to_owned(),
                        ReadResult::Buffer(Buffer::from(lines.join("\n"))),
                    )]),
                );
                let stats = Arc::new(RetryStats::default());
                let operator = OperatorBuilder::new(accessor.clone())
                    .layer(
                        RetryLayer::new()
                            .with_min_delay(Duration::ZERO)
                            .with_max_delay(Duration::ZERO)
                            .with_max_times(3)
                            .with_notify(DatabendRetryInterceptor::new(Some(stats.clone()))),
                    )
                    .finish();
                let deleter = SpillDeleter::new(operator, stats);
                accessor.fail_next_batches(1, kind);
                let pacing = if query_hook {
                    Pacing::Drain
                } else {
                    scheduled()
                };
                let outcome =
                    delete_by_meta(&deleter, "spill/", "spill/dir.list", usize::MAX, &pacing)
                        .await?
                        .outcome();
                assert!(!outcome.failed);
                assert_eq!(outcome.retried, 1);
                if query_hook {
                    assert_eq!(outcome.removed, lines.len());
                    assert_eq!(accessor.deleted_paths(), lines);
                    assert!(
                        accessor.written().is_empty(),
                        "drained manifest needs no rewrite"
                    );
                } else {
                    assert_eq!(outcome.removed, DELETE_BATCH_FILES);
                    assert_eq!(accessor.deleted_paths(), lines[..DELETE_BATCH_FILES]);
                    assert_eq!(accessor.written(), vec![(
                        "spill/dir.list".to_owned(),
                        lines[DELETE_BATCH_FILES..].join("\n"),
                    )]);
                }
                assert_eq!(outcome.charged, outcome.removed);
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resolve_dir_last_modified_from_meta_file() -> anyhow::Result<()> {
        let meta_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::new(),
            HashMap::from([
                (
                    "spill/dir/".to_string(),
                    StatResult::Meta(Box::new(dir_meta(None))),
                ),
                (
                    "spill/dir.list".to_string(),
                    StatResult::Meta(Box::new(file_meta(Some(meta_ts)))),
                ),
            ]),
            HashMap::new(),
        );
        let operator = OperatorBuilder::new(accessor).finish();

        let last_modified =
            resolve_dir_last_modified(&operator, &dir_meta(None), "spill/dir/", "spill/dir.list")
                .await;

        assert_eq!(last_modified, Some(meta_ts));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_meta_fresh_skips_directory_without_listing() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(now - 10_000)),
            )])]),
            HashMap::from([
                (
                    "spill/dir/".to_string(),
                    StatResult::Meta(Box::new(dir_meta(None))),
                ),
                (
                    "spill/dir.list".to_string(),
                    StatResult::Meta(Box::new(file_meta(Some(now)))),
                ),
            ]),
            HashMap::new(),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            expiry(now, Duration::from_secs(60)),
            100,
        )
        .await?;

        assert!(
            outcome.delete.is_none(),
            "a skipped directory must not feed the budget or the concurrency policy"
        );
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        assert_eq!(accessor.list_call_count(), 0);
        assert_eq!(accessor.read_call_count(), 0);
        Ok(())
    }

    /// When the store lists `x/` before `x.list`, the directory task claims the manifest and
    /// the top-level listing skips it, so an empty or unparsable manifest must be finished by
    /// the directory task itself, or an unlimited run would leave it behind.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_directory_task_drops_the_empty_manifest_it_claimed() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("")),
            )]),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(Some(old_ts)),
            expiry(now, Duration::from_secs(60)),
            100,
        )
        .await?;

        assert_eq!(outcome.delete.map(|d| d.removed), Some(3));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/".to_string(),
            "spill/dir.list".to_string(),
        ]);

        // Without a manifest there is nothing to finish but the directory.
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::new(),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(Some(old_ts)),
            expiry(now, Duration::from_secs(60)),
            100,
        )
        .await?;
        assert_eq!(outcome.delete.map(|d| d.removed), Some(2));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/".to_string(),
        ]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_meta_old_prefers_meta_delete_over_probe_or_list() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![
                ("spill/dir/file1".to_string(), file_meta(Some(old_ts))),
                ("spill/dir/file2".to_string(), file_meta(Some(old_ts))),
            ])]),
            HashMap::from([
                (
                    "spill/dir/".to_string(),
                    StatResult::Meta(Box::new(dir_meta(None))),
                ),
                (
                    "spill/dir.list".to_string(),
                    StatResult::Meta(Box::new(file_meta(Some(old_ts)))),
                ),
            ]),
            HashMap::from([(
                "spill/dir.list".to_string(),
                // A complete manifest, as written by `unload_spill_meta`: files, then itself,
                // then the directory.
                ReadResult::Buffer(Buffer::from(
                    "spill/dir/file1\nspill/dir/file2\nspill/dir.list\nspill/dir/",
                )),
            )]),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            expiry(now, Duration::from_secs(60)),
            100,
        )
        .await?;

        assert_eq!(outcome.delete.map(|d| d.removed), Some(4));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string(),
            "spill/dir.list".to_string(),
            "spill/dir/".to_string(),
        ]);
        assert_eq!(accessor.list_call_count(), 0);
        assert_eq!(accessor.read_call_count(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_throttled_meta_batch_is_deferred_not_retried_by_listing() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::from([(
                "spill/dir.list".to_string(),
                StatResult::Meta(Box::new(file_meta(Some(old_ts)))),
            )]),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("spill/dir/file1")),
            )]),
        );
        accessor.throttle_next_batches(1);
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            expiry(now, Duration::from_secs(60)),
            100,
        )
        .await?;

        let delete = outcome.delete.expect("a meta batch was attempted");
        assert_eq!(delete.removed, 0);
        assert!(
            delete.rate_limited,
            "the throttling signal must reach the scheduler"
        );
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        assert_eq!(
            accessor.list_call_count(),
            0,
            "no fallback listing under throttling"
        );
        Ok(())
    }

    /// On a real filesystem the directory mtime moves with every new file, so a live spill
    /// directory whose first file is old must not be aged from that file.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fs_live_directory_with_old_first_file_is_kept() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let dir = root.path().join("spill").join("dir");
        std::fs::create_dir_all(&dir)?;
        let old_file = dir.join("file0");
        std::fs::write(&old_file, b"x")?;
        std::fs::File::open(&old_file)?
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 3600))?;
        // Creating a second file now refreshes the directory mtime.
        std::fs::write(dir.join("file1"), b"y")?;

        let operator =
            Operator::new(opendal::services::Fs::default().root(root.path().to_str().unwrap()))?
                .finish();
        let outcome = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            expiry(Utc::now().timestamp_millis(), Duration::from_secs(60)),
            100,
        )
        .await?;

        assert!(outcome.delete.is_none(), "live directory must be kept");
        assert!(old_file.exists());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_manifest_is_deleted_in_bounded_batches() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let paths: Vec<String> = (0..2500).map(|i| format!("spill/dir/file{i}")).collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                StatResult::Meta(Box::new(file_meta(Some(old_ts)))),
            )]),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from(paths.join("\n"))),
            )]),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // No LIMIT: every path goes, but never more than DELETE_BATCH_FILES per batch.
        let outcome = delete_by_meta(
            &deleter,
            "spill/",
            "spill/dir.list",
            usize::MAX,
            &scheduled(),
        )
        .await?;
        assert!(matches!(outcome, MetaVacuum::Exhausted(o) if o.removed == 2500));
        assert_eq!(accessor.deleted_paths(), paths);
        assert_eq!(accessor.delete_call_count(), 3);
        // Nothing it named is left, on disk either.
        assert_eq!(accessor.written(), vec![(
            "spill/dir.list".to_string(),
            String::new()
        )]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_directory_is_listed_and_deleted_in_bounded_batches() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let files: Vec<_> = (0..2500)
            .map(|i| (format!("spill/dir/file{i:04}"), file_meta(Some(old_ts))))
            .collect();
        let mut expected: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
        expected.push("spill/dir/".to_string());
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), files)]),
            HashMap::new(),
            HashMap::new(),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // No LIMIT: the directory is drained in batches rather than buffered whole.
        let by_list = delete_by_list(&deleter, "spill/dir/", usize::MAX, &scheduled()).await?;
        assert_eq!(by_list.outcome.removed, 2501);
        assert!(by_list.finished);
        assert_eq!(accessor.deleted_paths(), expected);
        assert_eq!(accessor.delete_call_count(), 3);
        assert_eq!(
            accessor.list_call_count(),
            1,
            "one listing, consumed incrementally"
        );

        // The probe path shares the loop.
        let probed = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(Utc::now().timestamp_millis(), Duration::from_secs(60)),
            usize::MAX,
            &scheduled(),
        )
        .await?;
        assert_eq!(probed.map(|o| o.removed), Some(2501));
        assert_eq!(accessor.delete_call_count(), 6);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aborted_task_stops_between_batches() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let files: Vec<_> = (0..2500)
            .map(|i| (format!("spill/dir/file{i:04}"), file_meta(Some(old_ts))))
            .collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), files)]),
            HashMap::new(),
            HashMap::new(),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // The query is cancelled right after the first batch.
        let pacing = Pacing::Scheduled(AbortAfter::checks(0));
        let by_list = delete_by_list(&deleter, "spill/dir/", usize::MAX, &pacing).await?;
        assert_eq!(by_list.outcome.removed, DELETE_BATCH_FILES);
        assert!(!by_list.finished);
        assert_eq!(accessor.delete_call_count(), 1);
        Ok(())
    }

    /// A throttled listing batch ends the task even without `LIMIT`, so the scheduler sees the
    /// signal before the directory issues its next batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_throttled_listing_batch_ends_the_task() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let files: Vec<_> = (0..2500)
            .map(|i| (format!("spill/dir/file{i:04}"), file_meta(Some(old_ts))))
            .collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), files)]),
            HashMap::new(),
            HashMap::new(),
        );
        accessor.throttle_next_batches(1);
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        let by_list = delete_by_list(&deleter, "spill/dir/", usize::MAX, &scheduled()).await?;
        assert_eq!(by_list.outcome.removed, 0);
        assert!(by_list.outcome.rate_limited);
        assert!(!by_list.finished);
        assert_eq!(accessor.delete_call_count(), 1);
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        Ok(())
    }

    /// A manifest truncated after some valid paths but before the lines naming itself and the
    /// directory: the listed files go, then the directory is finished by listing and the
    /// manifest is dropped, so nothing is stranded behind the claim on it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_truncated_manifest_finishes_directory_by_listing() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![
                ("spill/dir/file1".to_string(), file_meta(Some(old_ts))),
                ("spill/dir/file2".to_string(), file_meta(Some(old_ts))),
            ])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("spill/dir/file1")),
            )]),
        );
        let deleter = Arc::new(SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
            scheduled(),
        )
        .await?;

        // The mock listing is static, so file1 shows up again in the listing pass.
        assert_eq!(outcome.delete.map(|d| d.removed), Some(5));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string(),
            "spill/dir/".to_string(),
            "spill/dir.list".to_string(),
        ]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_empty_manifest_falls_back_to_listing_and_drops_itself() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("")),
            )]),
        );
        let deleter = Arc::new(SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
            scheduled(),
        )
        .await?;

        assert_eq!(outcome.delete.map(|d| d.removed), Some(3));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/".to_string(),
            "spill/dir.list".to_string(),
        ]);
        Ok(())
    }

    /// A manifest names itself right before its directory marker. It must only be deleted in
    /// the batch that also takes everything after it, or the remainder would be written back
    /// into a manifest that was just removed and no run with the same `LIMIT` could finish it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_manifest_is_deleted_only_with_its_tail() -> anyhow::Result<()> {
        let accessor = MockVacuumAccessor::new(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("spill/dir.list\nspill/dir/\n")),
            )]),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // Budget for one object: neither line is taken by the manifest pass (the listing
        // fallback deals with the pair instead).
        let outcome = delete_by_meta(&deleter, "spill/", "spill/dir.list", 1, &scheduled()).await?;
        assert!(matches!(outcome, MetaVacuum::Nothing));
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());

        // Budget for two: both go in one batch.
        let outcome = delete_by_meta(&deleter, "spill/", "spill/dir.list", 2, &scheduled()).await?;
        assert!(matches!(outcome, MetaVacuum::Partial(o) if o.removed == 2));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir.list".to_string(),
            "spill/dir/".to_string(),
        ]);

        // The same holds at the batch-size boundary: 999 files, then the manifest and its
        // marker together in a second batch rather than the manifest alone in the first.
        let mut lines: Vec<String> = (0..DELETE_BATCH_FILES - 1)
            .map(|i| format!("spill/dir/file{i:04}"))
            .collect();
        lines.push("spill/dir.list".to_string());
        lines.push("spill/dir/".to_string());
        let accessor = MockVacuumAccessor::new(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from(lines.join("\n"))),
            )]),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );
        let outcome = delete_by_meta(
            &deleter,
            "spill/",
            "spill/dir.list",
            usize::MAX,
            &scheduled(),
        )
        .await?;
        assert!(matches!(outcome, MetaVacuum::Partial(o) if o.removed == DELETE_BATCH_FILES + 1));
        assert_eq!(accessor.delete_call_count(), 2);
        assert_eq!(accessor.deleted_paths(), lines);
        Ok(())
    }

    /// Backends accept fewer paths per request than a vacuum batch holds (GCS: 100). A batch is
    /// sent request by request so that a failure part-way still accounts for what went through,
    /// and the manifest is advanced exactly past those paths.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_partially_failed_batch_accounts_for_completed_requests() -> anyhow::Result<()> {
        let lines: Vec<String> = (0..5).map(|i| format!("spill/dir/file{i}")).collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from(lines.join("\n"))),
            )]),
        );
        accessor.set_delete_max_size(2);
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // Requests of 2, 2, 1; the second one fails.
        accessor.fail_batches_after(1, 1, opendal::ErrorKind::Unexpected);
        let outcome =
            delete_by_meta(&deleter, "spill/", "spill/dir.list", 100, &scheduled()).await?;
        let MetaVacuum::Partial(o) = outcome else {
            panic!("a failed request stops the task");
        };
        assert_eq!(o.removed, 2);
        assert_eq!(o.charged, 4, "the failed request is charged in full");
        assert!(o.failed);
        assert_eq!(accessor.deleted_paths(), lines[..2].to_vec());
        // The manifest is rewritten without the two paths that are gone.
        assert_eq!(accessor.written(), vec![(
            "spill/dir.list".to_string(),
            lines[2..].join("\n")
        )]);
        Ok(())
    }

    /// A request can delete some objects and still return an error. Only preceding,
    /// acknowledged requests advance the manifest; the failed request is charged in full
    /// and all its paths remain retryable, including any that were actually removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_unacknowledged_partial_deletion_is_charged_and_retried() -> anyhow::Result<()> {
        for kind in [
            opendal::ErrorKind::RateLimited,
            opendal::ErrorKind::Unexpected,
        ] {
            let lines: Vec<_> = (0..5).map(|i| format!("spill/dir/file{i}")).collect();
            let accessor = MockVacuumAccessor::new(
                HashMap::new(),
                HashMap::new(),
                HashMap::from([(
                    "spill/dir.list".to_owned(),
                    ReadResult::Buffer(Buffer::from(lines.join("\n"))),
                )]),
            );
            accessor.set_delete_max_size(2);
            // First request deletes files 0,1; second deletes file 2 but reports failure.
            accessor.partially_fail_batch_after(1, 1, kind);
            let progress = Arc::new(Mutex::new(Vec::new()));
            let sink = progress.clone();
            let deleter = SpillDeleter::with_report(
                OperatorBuilder::new(accessor.clone()).finish(),
                Arc::default(),
                Some(Arc::new(move |n| sink.lock().push(n))),
            );
            let outcome =
                delete_by_meta(&deleter, "spill/", "spill/dir.list", 100, &scheduled()).await?;
            let MetaVacuum::Partial(outcome) = outcome else {
                panic!("{kind:?}: a failed request must stop the task");
            };
            assert!(outcome.failed);
            assert_eq!(outcome.removed, 2, "only confirmed deletions are reported");
            assert_eq!(
                outcome.charged, 4,
                "both paths of the failed request consume budget"
            );
            assert_eq!(accessor.deleted_paths(), lines[..3]);
            assert_eq!(
                *progress.lock(),
                vec![2],
                "unacknowledged work is not progress"
            );
            let remaining = lines[2..].join("\n");
            assert_eq!(accessor.written(), vec![(
                "spill/dir.list".to_owned(),
                remaining.clone()
            )]);
            assert!(
                !accessor
                    .deleted_paths()
                    .iter()
                    .any(|p| p.ends_with(".list"))
            );

            // A later pass reads the rewritten manifest. It retries file 2 (safe even though
            // it was removed), finishes the remaining files and finally removes the metadata.
            let resumed = MockVacuumAccessor::new(
                HashMap::new(),
                HashMap::new(),
                HashMap::from([(
                    "spill/dir.list".to_owned(),
                    ReadResult::Buffer(Buffer::from(remaining)),
                )]),
            );
            resumed.set_delete_max_size(2);
            let deleter = Arc::new(SpillDeleter::new(
                OperatorBuilder::new(resumed.clone()).finish(),
                Arc::default(),
            ));
            let outcome = vacuum_meta_entry(
                deleter,
                Arc::from("spill/"),
                "spill/dir.list".to_owned(),
                100,
                scheduled(),
            )
            .await?;
            let outcome = outcome
                .delete
                .expect("resumed cleanup attempted the remaining paths");
            assert!(!outcome.failed);
            assert_eq!((outcome.removed, outcome.charged), (5, 5));
            let deleted = resumed.deleted_paths();
            assert!(deleted.starts_with(&lines[2..]));
            assert!(deleted.contains(&"spill/dir.list".to_owned()));
            assert!(deleted.contains(&"spill/dir/".to_owned()));
            assert!(!deleted.contains(&lines[0]) && !deleted.contains(&lines[1]));
        }
        Ok(())
    }

    /// Progress is reported after every request, not once per directory task, so a single
    /// large directory still shows movement in `system.processes`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_progress_is_reported_per_request() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let files: Vec<_> = (0..2500)
            .map(|i| (format!("spill/dir/file{i:04}"), file_meta(Some(old_ts))))
            .collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), files)]),
            HashMap::new(),
            HashMap::new(),
        );
        let reported = Arc::new(Mutex::new(Vec::new()));
        let sink = reported.clone();
        let deleter = SpillDeleter::with_report(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
            Some(Arc::new(move |n| sink.lock().push(n))),
        );

        let by_list = delete_by_list(&deleter, "spill/dir/", usize::MAX, &scheduled()).await?;
        assert_eq!(by_list.outcome.removed, 2501);
        assert_eq!(*reported.lock(), vec![1000, 2000, 2501]);

        // Concurrent tasks publish totals in increasing order: the counter and the callback
        // sit under one lock.
        reported.lock().clear();
        let deleter = Arc::new(deleter);
        let tasks = (0..8).map(|i| {
            let deleter = deleter.clone();
            async move {
                let paths = (0..300).map(|j| format!("spill/dir{i}/file{j}")).collect();
                deleter.delete(paths, "spill/").await
            }
        });
        futures::future::join_all(tasks).await;
        let seen = reported.lock().clone();
        assert_eq!(seen.len(), 8);
        assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
        assert_eq!(seen.last(), Some(&(2501 + 8 * 300)));
        Ok(())
    }

    /// A truncated manifest whose names use up the whole budget is rewritten empty, so the
    /// next run goes to the listing instead of re-deleting the same names forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_exhausted_manifest_is_rewritten_when_budget_runs_out() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("spill/dir/file1\n")),
            )]),
        );
        let deleter = Arc::new(SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            1,
            scheduled(),
        )
        .await?;

        assert_eq!(outcome.delete.map(|d| d.removed), Some(1));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string()
        ]);
        assert_eq!(accessor.written(), vec![(
            "spill/dir.list".to_string(),
            String::new()
        )]);
        Ok(())
    }

    /// The manifest dropped after a listing fallback is counted against the task's budget
    /// like any other object: a task never removes more than `max`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_listing_fallback_reserves_budget_for_the_manifest() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let new_accessor = |files: Vec<(String, Metadata)>| {
            MockVacuumAccessor::new(
                HashMap::from([("spill/dir/".to_string(), files)]),
                HashMap::new(),
                HashMap::from([(
                    "spill/dir.list".to_string(),
                    ReadResult::Buffer(Buffer::from("")),
                )]),
            )
        };
        let run = |accessor: Arc<MockVacuumAccessor>, max| {
            vacuum_meta_entry(
                Arc::new(SpillDeleter::new(
                    OperatorBuilder::new(accessor).finish(),
                    Arc::default(),
                )),
                Arc::from("spill/"),
                "spill/dir.list".to_string(),
                max,
                scheduled(),
            )
        };

        // Empty directory, empty manifest, budget for one object: the manifest goes, and only
        // because the directory was found empty.
        let accessor = new_accessor(vec![]);
        let outcome = run(accessor.clone(), 1).await?;
        assert_eq!(outcome.delete.map(|d| d.removed), Some(1));
        assert_eq!(accessor.deleted_paths(), vec!["spill/dir.list".to_string()]);

        // Same with a file left: the one slot goes to the file so `LIMIT 1` still makes
        // progress, and the manifest keeps the claim.
        let accessor = new_accessor(vec![(
            "spill/dir/file1".to_string(),
            file_meta(Some(old_ts)),
        )]);
        let outcome = run(accessor.clone(), 1).await?;
        assert_eq!(outcome.delete.map(|d| d.removed), Some(1));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string()
        ]);

        // Budget for two: the marker and the manifest, never a third object.
        let accessor = new_accessor(vec![]);
        let outcome = run(accessor.clone(), 2).await?;
        assert_eq!(outcome.delete.map(|d| d.removed), Some(2));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/".to_string(),
            "spill/dir.list".to_string(),
        ]);

        // One file, budget for two: file and marker fit, the manifest waits for the next run.
        let accessor = new_accessor(vec![(
            "spill/dir/file1".to_string(),
            file_meta(Some(old_ts)),
        )]);
        let outcome = run(accessor.clone(), 2).await?;
        assert_eq!(outcome.delete.map(|d| d.removed), Some(1));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string()
        ]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_throttled_listing_fallback_keeps_the_manifest() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("")),
            )]),
        );
        accessor.throttle_next_batches(1);
        let deleter = Arc::new(SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
            scheduled(),
        )
        .await?;

        let delete = outcome.delete.expect("the listing batch was attempted");
        assert_eq!(delete.removed, 0);
        assert!(delete.rate_limited);
        // No second request in the same task: the manifest stays for the next run.
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_failed_listing_fallback_keeps_the_manifest() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(old_ts)),
            )])]),
            HashMap::new(),
            HashMap::from([(
                "spill/dir.list".to_string(),
                ReadResult::Buffer(Buffer::from("")),
            )]),
        );
        accessor.fail_next_batches(1, opendal::ErrorKind::Unexpected);
        let deleter = Arc::new(SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
            scheduled(),
        )
        .await?;

        let delete = outcome.delete.expect("the listing batch was attempted");
        assert_eq!(delete.removed, 0);
        assert!(!delete.rate_limited);
        // Files are still there, so the manifest must survive for the next run.
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_probe_skips_fresh_directory() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(now)),
            )])]),
            HashMap::new(),
            HashMap::new(),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        let outcome = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(now, Duration::from_secs(60)),
            100,
            &scheduled(),
        )
        .await?;

        assert!(
            outcome.is_none(),
            "a fresh directory must not be reported as a clear delete batch"
        );
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        assert_eq!(accessor.list_call_count(), 1);
        assert_eq!(accessor.read_call_count(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_stops_at_budget_without_buffering_the_directory() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let files: Vec<_> = (0..5)
            .map(|i| (format!("spill/dir/file{i}"), file_meta(Some(old_ts))))
            .collect();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), files)]),
            HashMap::new(),
            HashMap::new(),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        // One unit of budget still makes progress.
        let by_list = delete_by_list(&deleter, "spill/dir/", 1, &scheduled()).await?;
        assert_eq!(by_list.outcome.removed, 1);
        assert!(!by_list.finished);
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file0".to_string()
        ]);

        let by_list = delete_by_list(&deleter, "spill/dir/", 2, &scheduled()).await?;
        assert_eq!(by_list.outcome.removed, 2);
        assert!(!by_list.finished);
        // Budget hit before the listing ended: no directory marker.
        assert_eq!(accessor.deleted_paths()[1..], [
            "spill/dir/file0".to_string(),
            "spill/dir/file1".to_string(),
        ]);

        let outcome = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(Utc::now().timestamp_millis(), Duration::from_secs(60)),
            3,
            &scheduled(),
        )
        .await?;
        assert_eq!(outcome.map(|o| o.removed), Some(3));
        assert_eq!(accessor.deleted_paths()[3..], [
            "spill/dir/file0".to_string(),
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string(),
        ]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_probe_reuses_probe_list_for_deletion() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![
                ("spill/dir/file1".to_string(), file_meta(Some(old_ts))),
                ("spill/dir/file2".to_string(), file_meta(Some(old_ts))),
            ])]),
            HashMap::new(),
            HashMap::new(),
        );
        let deleter = SpillDeleter::new(
            OperatorBuilder::new(accessor.clone()).finish(),
            Arc::default(),
        );

        let outcome = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(Utc::now().timestamp_millis(), Duration::from_secs(60)),
            100,
            &scheduled(),
        )
        .await?;

        assert_eq!(outcome.map(|o| o.removed), Some(3));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string(),
            "spill/dir/".to_string(),
        ]);
        assert_eq!(accessor.list_call_count(), 1);
        assert_eq!(accessor.read_call_count(), 0);
        Ok(())
    }
}

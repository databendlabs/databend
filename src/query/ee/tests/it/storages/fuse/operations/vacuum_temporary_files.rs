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
    use opendal::EntryMode;
    use opendal::Metadata;
    use opendal::OperatorBuilder;
    use opendal::raw::Access;
    use opendal::raw::AccessorInfo;
    use opendal::raw::MaybeSend;
    use opendal::raw::OpDelete;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpStat;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpStat;
    use opendal::raw::Timestamp;
    use opendal::raw::oio;
    use opendal::raw::oio::Entry;

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
        throttled_batches: Arc<AtomicUsize>,
    }

    impl oio::Delete for RecordingDeleter {
        fn delete(&mut self, path: &str, _args: OpDelete) -> opendal::Result<()> {
            self.pending.push(path.to_string());
            Ok(())
        }

        async fn flush(&mut self) -> opendal::Result<usize> {
            let throttled = self
                .throttled_batches
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok();
            if throttled {
                self.pending.clear();
                return Err(opendal::Error::new(
                    opendal::ErrorKind::RateLimited,
                    "mock SlowDown",
                ));
            }
            let n = self.pending.len();
            self.deleted.lock().append(&mut self.pending);
            Ok(n)
        }
    }

    #[derive(Debug)]
    struct MockVacuumAccessor {
        list_entries: HashMap<String, Vec<(String, Metadata)>>,
        stat_results: HashMap<String, StatResult>,
        read_results: HashMap<String, ReadResult>,
        deleted: Arc<Mutex<Vec<String>>>,
        throttled_batches: Arc<AtomicUsize>,
        list_calls: AtomicUsize,
        read_calls: AtomicUsize,
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
                throttled_batches: Arc::new(AtomicUsize::new(0)),
                list_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
            })
        }

        fn throttle_next_batches(&self, n: usize) {
            self.throttled_batches.store(n, Ordering::Release);
        }

        fn deleted_paths(&self) -> Vec<String> {
            self.deleted.lock().clone()
        }

        fn list_call_count(&self) -> usize {
            self.list_calls.load(Ordering::Acquire)
        }

        fn read_call_count(&self) -> usize {
            self.read_calls.load(Ordering::Acquire)
        }
    }

    impl Access for MockVacuumAccessor {
        type Reader = Buffer;
        type Writer = ();
        type Lister = VecLister;
        type Deleter = RecordingDeleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                stat: true,
                read: true,
                list: true,
                delete: true,
                delete_max_size: Some(1000),
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

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            Ok((RpDelete::default(), RecordingDeleter {
                deleted: self.deleted.clone(),
                pending: Vec::new(),
                throttled_batches: self.throttled_batches.clone(),
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
            Arc::new(SpillDeleter::new(operator.clone())),
            temporary_dir.into(),
            dir_path.to_owned(),
            metadata.clone(),
            expiry,
            max,
        )
        .await
    }

    fn clear() -> DeleteOutcome {
        DeleteOutcome {
            removed: 1,
            rate_limited: false,
            retried: 0,
        }
    }

    fn rate_limited() -> DeleteOutcome {
        DeleteOutcome {
            removed: 0,
            rate_limited: true,
            retried: 0,
        }
    }

    fn retried(n: usize) -> DeleteOutcome {
        DeleteOutcome {
            removed: 1,
            rate_limited: false,
            retried: n,
        }
    }

    #[test]
    fn test_adaptive_concurrency_ramps_up_once_per_full_streak() {
        let c = AdaptiveConcurrency::new();
        let t0 = Instant::now();
        assert_eq!(c.current(), AdaptiveConcurrency::INIT);

        for _ in 0..AdaptiveConcurrency::RAMP_UP_BATCHES - 1 {
            c.observe(&clear(), t0);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::INIT);
        c.observe(&clear(), t0);
        assert_eq!(c.current(), AdaptiveConcurrency::INIT + 1);

        for _ in 0..AdaptiveConcurrency::RAMP_UP_BATCHES - 1 {
            c.observe(&clear(), t0);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::INIT + 1);

        for _ in 0..AdaptiveConcurrency::MAX * AdaptiveConcurrency::RAMP_UP_BATCHES {
            c.observe(&clear(), t0);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::MAX);
    }

    #[test]
    fn test_adaptive_concurrency_halves_at_most_once_per_cooldown() {
        let c = AdaptiveConcurrency::new();
        let t0 = Instant::now();
        let cooldown = AdaptiveConcurrency::DECREASE_COOLDOWN;

        // Same episode within the cooldown: one decrease.
        c.observe(&rate_limited(), t0);
        c.observe(&rate_limited(), t0);
        c.observe(&rate_limited(), t0 + Duration::from_secs(3));
        c.observe(&rate_limited(), t0 + cooldown - Duration::from_millis(1));
        assert_eq!(c.current(), AdaptiveConcurrency::INIT / 2);

        c.observe(&rate_limited(), t0 + cooldown);
        assert_eq!(c.current(), AdaptiveConcurrency::INIT / 4);

        for i in 2..20 {
            c.observe(&rate_limited(), t0 + cooldown * i);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::MIN);

        // Throttling restarts the streak.
        for _ in 0..AdaptiveConcurrency::RAMP_UP_BATCHES - 1 {
            c.observe(&clear(), t0);
        }
        c.observe(&rate_limited(), t0 + cooldown * 30);
        for _ in 0..AdaptiveConcurrency::RAMP_UP_BATCHES - 1 {
            c.observe(&clear(), t0);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::MIN);
        c.observe(&clear(), t0);
        assert_eq!(c.current(), AdaptiveConcurrency::MIN + 1);
    }

    #[test]
    fn test_adaptive_concurrency_ignores_sporadic_retries() {
        let c = AdaptiveConcurrency::new();
        let init = AdaptiveConcurrency::INIT;

        // Below a full INIT round of retries is noise, whatever the bound.
        assert!(!c.is_throttled(&retried(init - 1)));
        assert!(c.is_throttled(&retried(init)));
        assert!(c.is_throttled(&rate_limited()));

        // Above INIT the threshold scales with the bound.
        let t0 = Instant::now();
        for _ in 0..(AdaptiveConcurrency::MAX - init) * AdaptiveConcurrency::RAMP_UP_BATCHES {
            c.observe(&clear(), t0);
        }
        assert_eq!(c.current(), AdaptiveConcurrency::MAX);
        assert!(!c.is_throttled(&retried(AdaptiveConcurrency::MAX / 2 - 1)));
        assert!(c.is_throttled(&retried(AdaptiveConcurrency::MAX / 2)));
    }

    #[test]
    fn test_run_budget_reports_running_total() {
        let reported = Arc::new(Mutex::new(Vec::new()));
        let sink = reported.clone();
        let mut budget = RunBudget::new(10, Some(Arc::new(move |total| sink.lock().push(total))));
        budget.record(3);
        budget.record(4);
        assert_eq!(budget.removed(), 7);
        assert_eq!(budget.remaining(), 3);
        assert!(!budget.exhausted());
        budget.record(3);
        assert!(budget.exhausted());
        assert_eq!(budget.remaining(), 0);
        assert_eq!(*reported.lock(), vec![3, 7, 10]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rate_limited_batch_is_not_counted() -> anyhow::Result<()> {
        let accessor = MockVacuumAccessor::new(HashMap::new(), HashMap::new(), HashMap::new());
        let deleter = SpillDeleter::new(OperatorBuilder::new(accessor.clone()).finish());
        let paths = vec!["spill/dir/file1".to_string(), "spill/dir/file2".to_string()];

        accessor.throttle_next_batches(1);
        let outcome = deleter.delete(paths.clone(), "spill/dir/").await;
        assert_eq!(outcome.removed, 0);
        assert!(outcome.rate_limited);
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());

        let outcome = deleter.delete(paths.clone(), "spill/dir/").await;
        assert_eq!(outcome.removed, 2);
        assert!(!outcome.rate_limited);
        assert_eq!(accessor.deleted_paths(), paths);
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
        let deleter = SpillDeleter::new(OperatorBuilder::new(accessor.clone()).finish());

        // No LIMIT: every path goes, but never more than META_BATCH_FILES per batch.
        let outcome = delete_by_meta(&deleter, "spill/", "spill/dir.list", usize::MAX).await?;
        assert!(matches!(outcome, MetaVacuum::Exhausted(o) if o.removed == 2500));
        assert_eq!(accessor.deleted_paths(), paths);
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
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
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
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
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
        ));

        let outcome = vacuum_meta_entry(
            deleter,
            Arc::from("spill/"),
            "spill/dir.list".to_string(),
            100,
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
        let deleter = SpillDeleter::new(OperatorBuilder::new(accessor.clone()).finish());

        let outcome = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(now, Duration::from_secs(60)),
            100,
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
        let deleter = SpillDeleter::new(OperatorBuilder::new(accessor.clone()).finish());

        // One unit of budget still makes progress.
        let outcome = delete_by_list(&deleter, "spill/dir/", 1).await?;
        assert_eq!(outcome.map(|o| o.removed), Some(1));
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file0".to_string()
        ]);

        let outcome = delete_by_list(&deleter, "spill/dir/", 2).await?;
        assert_eq!(outcome.map(|o| o.removed), Some(2));
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
        let deleter = SpillDeleter::new(OperatorBuilder::new(accessor.clone()).finish());

        let outcome = delete_by_probe(
            &deleter,
            "spill/dir/",
            expiry(Utc::now().timestamp_millis(), Duration::from_secs(60)),
            100,
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

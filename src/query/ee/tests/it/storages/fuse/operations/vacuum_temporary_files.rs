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

    /// Drive the real per-directory unit of work and return how many files it removed.
    async fn run_dir_vacuum_for_test(
        operator: &Operator,
        temporary_dir: &str,
        dir_path: &str,
        metadata: &Metadata,
        timestamp: i64,
        expire_time: i64,
        limit: usize,
    ) -> Result<usize> {
        let progress = Arc::new(VacuumProgress::new(None));
        let gc_metas = Arc::new(Mutex::new(HashSet::new()));
        let name = dir_path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        vacuum_dir_entry(
            operator.clone(),
            temporary_dir.to_string(),
            dir_path.to_string(),
            name,
            metadata.clone(),
            timestamp,
            expire_time,
            limit,
            progress.clone(),
            gc_metas,
        )
        .await?;
        Ok(progress.removed())
    }

    fn controller() -> (DirConcurrency, Instant) {
        let t0 = Instant::now();
        (DirConcurrency::new(t0), t0)
    }

    #[test]
    fn test_dir_concurrency_ramps_up_once_per_full_streak() {
        let (dirs, t0) = controller();
        assert_eq!(dirs.current(), DirConcurrency::INIT);

        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        assert_eq!(dirs.current(), DirConcurrency::INIT);
        dirs.observe(ThrottleSignal::Clear, t0);
        assert_eq!(dirs.current(), DirConcurrency::INIT + 1);

        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        assert_eq!(dirs.current(), DirConcurrency::INIT + 1);

        for _ in 0..DirConcurrency::MAX * DirConcurrency::RAMP_UP_BATCHES {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        assert_eq!(dirs.current(), DirConcurrency::MAX);
    }

    #[test]
    fn test_dir_concurrency_light_throttling_holds_and_restarts_streak() {
        let (dirs, t0) = controller();

        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        dirs.observe(ThrottleSignal::Light, t0);
        assert_eq!(
            dirs.current(),
            DirConcurrency::INIT,
            "light throttling keeps the bound"
        );

        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        assert_eq!(dirs.current(), DirConcurrency::INIT);
        dirs.observe(ThrottleSignal::Clear, t0);
        assert_eq!(dirs.current(), DirConcurrency::INIT + 1);
    }

    #[test]
    fn test_dir_concurrency_halves_at_most_once_per_cooldown() {
        let (dirs, t0) = controller();
        let cooldown = DirConcurrency::DECREASE_COOLDOWN;

        // Same episode within the cooldown: one decrease.
        dirs.observe(ThrottleSignal::Heavy, t0);
        dirs.observe(ThrottleSignal::Heavy, t0);
        dirs.observe(ThrottleSignal::Heavy, t0 + Duration::from_secs(3));
        dirs.observe(
            ThrottleSignal::Heavy,
            t0 + cooldown - Duration::from_millis(1),
        );
        assert_eq!(dirs.current(), DirConcurrency::INIT / 2);

        dirs.observe(ThrottleSignal::Heavy, t0 + cooldown);
        assert_eq!(dirs.current(), DirConcurrency::INIT / 4);
        for i in 2..10 {
            dirs.observe(ThrottleSignal::Heavy, t0 + cooldown * i);
        }
        assert_eq!(dirs.current(), DirConcurrency::MIN);

        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        dirs.observe(ThrottleSignal::Heavy, t0 + cooldown * 20);
        for _ in 0..DirConcurrency::RAMP_UP_BATCHES - 1 {
            dirs.observe(ThrottleSignal::Clear, t0);
        }
        assert_eq!(dirs.current(), DirConcurrency::MIN);
    }

    #[test]
    fn test_classify_delete() {
        let ok: opendal::Result<()> = Ok(());
        let rate_limited: opendal::Result<()> = Err(opendal::Error::new(
            opendal::ErrorKind::RateLimited,
            "mock SlowDown",
        ));
        let other: opendal::Result<()> =
            Err(opendal::Error::new(opendal::ErrorKind::Unexpected, "boom"));

        assert_eq!(classify_delete(&ok, 0, 8), ThrottleSignal::Clear);
        assert_eq!(classify_delete(&other, 0, 8), ThrottleSignal::Clear);
        assert_eq!(classify_delete(&ok, 1, 8), ThrottleSignal::Light);
        assert_eq!(classify_delete(&ok, 3, 8), ThrottleSignal::Light);
        assert_eq!(classify_delete(&ok, 4, 8), ThrottleSignal::Heavy);
        assert_eq!(classify_delete(&ok, 1, 1), ThrottleSignal::Heavy);
        assert_eq!(classify_delete(&rate_limited, 0, 32), ThrottleSignal::Heavy);
    }

    #[test]
    fn test_removed_files_are_mirrored_into_progress_sink() {
        let sink = Arc::new(Progress::create());
        let progress = VacuumProgress::new(Some(sink.clone()));
        assert_eq!(progress.add_removed(3), 3);
        assert_eq!(progress.add_removed(4), 7);
        assert_eq!(progress.removed(), 7);
        assert_eq!(sink.get_values().rows, 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rate_limited_batch_is_not_counted_and_lowers_concurrency() -> anyhow::Result<()> {
        let accessor = MockVacuumAccessor::new(HashMap::new(), HashMap::new(), HashMap::new());
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let progress = VacuumProgress::new(None);

        accessor.throttle_next_batches(1);
        let removed = delete_counted(
            &operator,
            vec!["spill/dir/file1".to_string(), "spill/dir/file2".to_string()],
            "spill/dir/",
            &progress,
        )
        .await;
        assert_eq!(removed, 0);
        assert_eq!(progress.concurrency(), DirConcurrency::INIT / 2);
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());

        let removed = delete_counted(
            &operator,
            vec!["spill/dir/file1".to_string(), "spill/dir/file2".to_string()],
            "spill/dir/",
            &progress,
        )
        .await;
        assert_eq!(removed, 2);
        assert_eq!(progress.concurrency(), DirConcurrency::INIT / 2);
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string()
        ]);
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
            resolve_dir_last_modified(&operator, &dir_meta(None), "spill/dir.list").await;

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
        let removed = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            now,
            Duration::from_secs(60).as_millis() as i64,
            100,
        )
        .await?;

        assert_eq!(removed, 0);
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
                ReadResult::Buffer(Buffer::from("spill/dir/file1\nspill/dir/file2")),
            )]),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let removed = run_dir_vacuum_for_test(
            &operator,
            "spill/",
            "spill/dir/",
            &dir_meta(None),
            now,
            Duration::from_secs(60).as_millis() as i64,
            100,
        )
        .await?;

        assert_eq!(removed, 2);
        assert_eq!(accessor.deleted_paths(), vec![
            "spill/dir/file1".to_string(),
            "spill/dir/file2".to_string()
        ]);
        assert_eq!(accessor.list_call_count(), 0);
        assert_eq!(accessor.read_call_count(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_dir_with_probe_skips_fresh_directory() -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![(
                "spill/dir/file1".to_string(),
                file_meta(Some(now)),
            )])]),
            HashMap::new(),
            HashMap::new(),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let progress = VacuumProgress::new(None);

        let removed = vacuum_dir_with_probe(
            &operator,
            "spill/dir/",
            now,
            Duration::from_secs(60).as_millis() as i64,
            100,
            &progress,
        )
        .await?;

        assert_eq!(removed, 0);
        assert_eq!(progress.removed(), 0);
        assert_eq!(accessor.deleted_paths(), Vec::<String>::new());
        assert_eq!(accessor.list_call_count(), 1);
        assert_eq!(accessor.read_call_count(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_dir_with_probe_reuses_probe_list_for_deletion() -> anyhow::Result<()> {
        let old_ts = (Utc::now() - ChronoDuration::hours(2)).timestamp_millis();
        let accessor = MockVacuumAccessor::new(
            HashMap::from([("spill/dir/".to_string(), vec![
                ("spill/dir/file1".to_string(), file_meta(Some(old_ts))),
                ("spill/dir/file2".to_string(), file_meta(Some(old_ts))),
            ])]),
            HashMap::new(),
            HashMap::new(),
        );
        let operator = OperatorBuilder::new(accessor.clone()).finish();
        let progress = VacuumProgress::new(None);

        let removed = vacuum_dir_with_probe(
            &operator,
            "spill/dir/",
            Utc::now().timestamp_millis(),
            Duration::from_secs(60).as_millis() as i64,
            100,
            &progress,
        )
        .await?;

        assert_eq!(removed, 3);
        assert_eq!(progress.removed(), 3);
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

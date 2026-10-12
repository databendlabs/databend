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
    }

    impl oio::Delete for RecordingDeleter {
        fn delete(&mut self, path: &str, _args: OpDelete) -> opendal::Result<()> {
            self.deleted.lock().push(path.to_string());
            Ok(())
        }

        async fn flush(&mut self) -> opendal::Result<usize> {
            Ok(self.deleted.lock().len())
        }
    }

    #[derive(Debug)]
    struct MockVacuumAccessor {
        list_entries: HashMap<String, Vec<(String, Metadata)>>,
        stat_results: HashMap<String, StatResult>,
        read_results: HashMap<String, ReadResult>,
        deleted: Arc<Mutex<Vec<String>>>,
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
                list_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
            })
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
        let removed_total = Arc::new(AtomicUsize::new(0));
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
            removed_total.clone(),
            gc_metas,
        )
        .await?;
        Ok(removed_total.load(Ordering::Relaxed))
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
        let removed_total = AtomicUsize::new(0);

        let removed = vacuum_dir_with_probe(
            &operator,
            "spill/dir/",
            now,
            Duration::from_secs(60).as_millis() as i64,
            100,
            &removed_total,
        )
        .await?;

        assert_eq!(removed, 0);
        assert_eq!(removed_total.load(Ordering::Relaxed), 0);
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
        let removed_total = AtomicUsize::new(0);

        let removed = vacuum_dir_with_probe(
            &operator,
            "spill/dir/",
            Utc::now().timestamp_millis(),
            Duration::from_secs(60).as_millis() as i64,
            100,
            &removed_total,
        )
        .await?;

        assert_eq!(removed, 3);
        assert_eq!(removed_total.load(Ordering::Relaxed), 3);
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

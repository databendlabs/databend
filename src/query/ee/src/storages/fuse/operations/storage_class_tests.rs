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

mod real_s3 {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use anyhow::Context;
    use chrono::Utc;
    use databend_common_catalog::table::Table;
    use databend_common_catalog::table_context::TableContextTableAccess;
    use databend_common_meta_app::storage::StorageParams;
    use databend_common_meta_app::storage::StorageS3Config;
    use databend_common_storages_fuse::FuseTable;
    use databend_common_storages_fuse::io::SegmentsIO;
    use databend_query::test_kits::TestFixture;
    use databend_storages_common_table_meta::meta::CompactSegmentInfo;
    use futures::TryStreamExt;
    use opendal::Buffer;
    use opendal::Operator;
    use opendal::layers::HttpClientLayer;
    use opendal::raw::HttpBody;
    use opendal::raw::HttpClient;
    use opendal::raw::HttpFetch;
    use opendal::services::S3;

    use crate::test_kits::context::EESetup;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ObjectKind {
        Snapshot,
        Segment,
        Block,
    }

    type TableObjects = BTreeMap<String, ObjectKind>;
    type HeadResponse = Arc<Mutex<Option<(String, String)>>>;

    /// Observe real AWS HeadObject responses, without mocking or changing the SQL writer.
    /// OpenDAL's Metadata does not expose the S3 StorageClass field.
    struct HeadObserver {
        client: HttpClient,
        response: HeadResponse,
    }

    impl HttpFetch for HeadObserver {
        async fn fetch(
            &self,
            request: http::Request<Buffer>,
        ) -> opendal::Result<http::Response<HttpBody>> {
            let is_head = request.method() == http::Method::HEAD;
            let path = request.uri().path().to_owned();
            let response = self.client.fetch(request).await?;
            if is_head && response.status().is_success() {
                // AWS omits x-amz-storage-class for STANDARD objects. Do not normalize
                // errors or missing HEAD responses to STANDARD.
                let class = response
                    .headers()
                    .get("x-amz-storage-class")
                    .map(|value| value.to_str())
                    .transpose()
                    .map_err(|e| {
                        opendal::Error::new(opendal::ErrorKind::Unexpected, e.to_string())
                    })?
                    .unwrap_or("STANDARD")
                    .to_owned();
                *self.response.lock().unwrap() = Some((path, class));
            }
            Ok(response)
        }
    }

    #[tokio::test]
    async fn head_observer_preserves_real_response_semantics() -> anyhow::Result<()> {
        struct TestClient {
            status: u16,
            class: Option<&'static str>,
        }

        impl HttpFetch for TestClient {
            async fn fetch(
                &self,
                _request: http::Request<Buffer>,
            ) -> opendal::Result<http::Response<HttpBody>> {
                let mut response = http::Response::builder().status(self.status);
                if let Some(class) = self.class {
                    response = response.header("x-amz-storage-class", class);
                }
                Ok(response
                    .body(HttpBody::new(futures::stream::empty(), Some(0)))
                    .unwrap())
            }
        }

        for (method, status, header, expected) in [
            (
                http::Method::HEAD,
                200,
                Some("INTELLIGENT_TIERING"),
                Some("INTELLIGENT_TIERING"),
            ),
            (http::Method::HEAD, 200, None, Some("STANDARD")),
            (http::Method::HEAD, 403, None, None),
            (http::Method::GET, 200, None, None),
        ] {
            let recorded = HeadResponse::default();
            let observer = HeadObserver {
                client: HttpClient::with(TestClient {
                    status,
                    class: header,
                }),
                response: recorded.clone(),
            };
            let request = http::Request::builder()
                .method(method)
                .uri("https://bucket.s3.us-east-2.amazonaws.com/object")
                .body(Buffer::new())?;
            let response = observer.fetch(request).await?;
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(
                response
                    .headers()
                    .get("x-amz-storage-class")
                    .map(|v| v.to_str().unwrap()),
                header
            );
            assert_eq!(
                recorded
                    .lock()
                    .unwrap()
                    .take()
                    .map(|(_, class)| class)
                    .as_deref(),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn new_objects_requires_each_immutable_object_kind() -> anyhow::Result<()> {
        let previous = BTreeMap::from([
            ("old-snapshot".to_owned(), ObjectKind::Snapshot),
            ("old-segment".to_owned(), ObjectKind::Segment),
            ("old-block".to_owned(), ObjectKind::Block),
        ]);
        let new = BTreeMap::from([
            ("new-snapshot".to_owned(), ObjectKind::Snapshot),
            ("new-segment".to_owned(), ObjectKind::Segment),
            ("new-block".to_owned(), ObjectKind::Block),
        ]);
        let mut current = previous.clone();
        current.extend(new.clone());
        assert_eq!(new_objects(&current, &previous)?, new);
        current.remove("new-block");
        assert!(new_objects(&current, &previous).is_err());
        Ok(())
    }

    async fn table_objects(
        fixture: &TestFixture,
        table_name: &str,
    ) -> anyhow::Result<TableObjects> {
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx
            .get_table("default", "storage_class", table_name)
            .await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table
            .read_table_snapshot_without_cache()
            .await?
            .context("INSERT must create a snapshot")?;
        let location = table
            .snapshot_loc()
            .context("snapshot location must be present")?;
        let mut objects = BTreeMap::from([(location, ObjectKind::Snapshot)]);
        anyhow::ensure!(!snapshot.segments.is_empty(), "INSERT must create segments");
        let segments_io = SegmentsIO::create(ctx, table.get_operator(), table.schema());
        let segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(&snapshot.segments, false)
            .await?;
        for ((location, _), segment) in snapshot.segments.iter().zip(segments) {
            objects.insert(location.clone(), ObjectKind::Segment);
            let blocks = segment?.block_metas()?;
            anyhow::ensure!(!blocks.is_empty(), "INSERT must create blocks");
            for block in blocks {
                objects.insert(block.location.0.clone(), ObjectKind::Block);
            }
        }
        // Inspect immutable objects referenced by the current snapshot, not mutable
        // hint files or unrelated keys. This also lets SET/UNSET distinguish new objects.
        Ok(objects)
    }

    async fn check_objects(
        operator: &Operator,
        response: &HeadResponse,
        objects: &TableObjects,
        expected: &str,
    ) -> anyhow::Result<()> {
        for (path, kind) in objects {
            *response.lock().unwrap() = None;
            operator
                .stat(path)
                .await
                .with_context(|| format!("HeadObject for {kind:?}: {path}"))?;
            let (request_path, actual) = response
                .lock()
                .unwrap()
                .take()
                .context("stat must issue a successful real HeadObject request")?;
            anyhow::ensure!(
                request_path.ends_with(&format!("/{path}")),
                "unexpected HEAD path: {request_path}"
            );
            anyhow::ensure!(
                actual == expected,
                "{kind:?} {path}: expected {expected}, AWS returned {actual}"
            );
        }
        Ok(())
    }

    fn new_objects(
        current: &TableObjects,
        previous: &TableObjects,
    ) -> anyhow::Result<TableObjects> {
        let objects: TableObjects = current
            .iter()
            .filter(|(path, _)| !previous.contains_key(*path))
            .map(|(path, kind)| (path.clone(), *kind))
            .collect();
        for kind in [ObjectKind::Snapshot, ObjectKind::Segment, ObjectKind::Block] {
            anyhow::ensure!(
                objects.values().any(|actual| *actual == kind),
                "INSERT must produce a new {kind:?}"
            );
        }
        Ok(objects)
    }

    async fn check_data(
        fixture: &TestFixture,
        table: &str,
        expected: &[i64],
    ) -> anyhow::Result<()> {
        // Select actual rows instead of aggregates that could be answered from statistics.
        let blocks: Vec<_> = fixture
            .execute_query(&format!("SELECT id FROM storage_class.{table} ORDER BY id"))
            .await?
            .try_collect()
            .await?;
        let mut row = 0;
        for block in blocks {
            for index in 0..block.num_rows() {
                let expected_value = expected.get(row).context("unexpected extra row")?;
                let actual = block
                    .get_by_offset(0)
                    .value()
                    .index(index)
                    .context("missing id")?
                    .to_string();
                anyhow::ensure!(
                    actual == expected_value.to_string(),
                    "{table} row {row}: expected {expected_value}, got {actual}"
                );
                row += 1;
            }
        }
        anyhow::ensure!(
            row == expected.len(),
            "{table}: expected {} rows, got {row}",
            expected.len()
        );
        Ok(())
    }

    async fn sql_roundtrip(
        storage: StorageParams,
        operator: &Operator,
        response: &HeadResponse,
    ) -> anyhow::Result<()> {
        let mut setup = EESetup::new();
        setup.config_mut().storage.params = storage;
        setup.config_mut().storage.allow_insecure = false;
        let fixture = TestFixture::setup_with_custom(setup).await?;
        // Keep old blocks/segments intact so a second write cannot compact away
        // the objects whose original storage class we recheck.
        fixture
            .execute_command("SET auto_compaction_imperfect_blocks_threshold = 0")
            .await?;
        fixture.execute_command("CREATE DATABASE storage_class OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'intelligent_tiering')").await?;
        fixture
            .execute_command("SET s3_storage_class = 'standard'")
            .await?;
        fixture
            .execute_command("CREATE TABLE storage_class.inherited(id INT)")
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.inherited VALUES (1)")
            .await?;
        let inherited = table_objects(&fixture, "inherited").await?;
        check_objects(operator, response, &inherited, "INTELLIGENT_TIERING").await?;
        check_data(&fixture, "inherited", &[1]).await?;

        // Explicit STANDARD overrides both the database default and the session.
        fixture
            .execute_command("SET s3_storage_class = 'intelligent_tiering'")
            .await?;
        fixture
            .execute_command(
                "CREATE TABLE storage_class.explicit(id INT) S3_STORAGE_CLASS = 'standard'",
            )
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.explicit VALUES (2)")
            .await?;
        check_objects(
            operator,
            response,
            &table_objects(&fixture, "explicit").await?,
            "STANDARD",
        )
        .await?;
        check_data(&fixture, "explicit", &[2]).await?;

        // SET changes future writes only: inspect new objects and recheck the old ones.
        fixture
            .execute_command(
                "ALTER TABLE storage_class.inherited SET OPTIONS(S3_STORAGE_CLASS = 'standard')",
            )
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.inherited VALUES (3)")
            .await?;
        let after_set = table_objects(&fixture, "inherited").await?;
        let standard = new_objects(&after_set, &inherited)?;
        check_objects(operator, response, &standard, "STANDARD").await?;
        check_objects(operator, response, &inherited, "INTELLIGENT_TIERING").await?;
        check_data(&fixture, "inherited", &[1, 3]).await?;

        // UNSET removes the persisted default and falls back to the current session.
        fixture
            .execute_command("ALTER TABLE storage_class.inherited UNSET OPTIONS(S3_STORAGE_CLASS)")
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.inherited VALUES (5)")
            .await?;
        let after_unset = table_objects(&fixture, "inherited").await?;
        check_objects(
            operator,
            response,
            &new_objects(&after_unset, &after_set)?,
            "INTELLIGENT_TIERING",
        )
        .await?;
        check_objects(operator, response, &standard, "STANDARD").await?;
        check_data(&fixture, "inherited", &[1, 3, 5]).await?;

        // Creation-time inheritance must survive a database change, even when the
        // session and the new database default both say STANDARD.
        fixture
            .execute_command("CREATE TABLE storage_class.before_alter(id INT)")
            .await?;
        fixture
            .execute_command(
                "ALTER DATABASE storage_class SET OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'standard')",
            )
            .await?;
        fixture
            .execute_command("SET s3_storage_class = 'standard'")
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.before_alter VALUES (7)")
            .await?;
        check_objects(
            operator,
            response,
            &table_objects(&fixture, "before_alter").await?,
            "INTELLIGENT_TIERING",
        )
        .await?;
        check_data(&fixture, "before_alter", &[7]).await?;

        fixture
            .execute_command("SET s3_storage_class = 'intelligent_tiering'")
            .await?;
        fixture
            .execute_command("CREATE TABLE storage_class.after_alter(id INT)")
            .await?;
        fixture
            .execute_command("INSERT INTO storage_class.after_alter VALUES (11)")
            .await?;
        check_objects(
            operator,
            response,
            &table_objects(&fixture, "after_alter").await?,
            "STANDARD",
        )
        .await?;
        check_data(&fixture, "after_alter", &[11]).await?;

        // Temporary tables must override even an explicit INTELLIGENT_TIERING option.
        fixture.execute_command("CREATE TEMPORARY TABLE storage_class.temp(id INT) S3_STORAGE_CLASS = 'intelligent_tiering'").await?;
        fixture
            .execute_command("INSERT INTO storage_class.temp VALUES (13)")
            .await?;
        check_objects(
            operator,
            response,
            &table_objects(&fixture, "temp").await?,
            "STANDARD",
        )
        .await?;
        check_data(&fixture, "temp", &[13]).await?;
        fixture
            .execute_command("DROP TABLE storage_class.temp")
            .await?;
        fixture
            .execute_command("DROP DATABASE storage_class")
            .await?;
        Ok(())
    }

    /// SQL -> FUSE -> AWS S3, including actual stored StorageClass and data readback.
    /// Run with DATABEND_TEST_S3_BUCKET/REGION and optional DATABEND_TEST_S3_ROOT;
    /// credentials come from the AWS credential chain, never from SQL literals.
    /// The identity needs list/get/put/delete access to the test prefix.
    ///
    /// ```text
    /// cargo test -p databend-enterprise-query --lib real_s3::storage_class_sql_roundtrip -- --ignored
    /// ```
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn storage_class_sql_roundtrip() -> anyhow::Result<()> {
        let bucket = std::env::var("DATABEND_TEST_S3_BUCKET")?;
        let region = std::env::var("DATABEND_TEST_S3_REGION")?;
        anyhow::ensure!(
            !bucket.is_empty() && !region.is_empty(),
            "bucket and region must not be empty"
        );
        let configured_root = std::env::var("DATABEND_TEST_S3_ROOT").unwrap_or_default();
        let configured_root = configured_root.trim_matches('/');
        let test_id = databend_storages_common_table_meta::meta::uuid_from_date_time(Utc::now())
            .simple()
            .to_string();
        let suffix = format!("storage-class/{test_id}/");
        let root = if configured_root.is_empty() {
            suffix
        } else {
            format!("{configured_root}/{suffix}")
        };
        let endpoint = format!("https://s3.{region}.amazonaws.com");
        let response = HeadResponse::default();
        let observer = HeadObserver {
            client: HttpClient::new()?,
            response: response.clone(),
        };
        let operator = Operator::new(
            S3::default()
                .bucket(&bucket)
                .region(&region)
                .endpoint(&endpoint)
                .root(&root),
        )?
        .finish()
        .layer(HttpClientLayer::new(HttpClient::with(observer)));
        let storage = StorageParams::S3(StorageS3Config {
            bucket,
            region,
            root: root.clone(),
            endpoint_url: endpoint,
            allow_credential_chain: Some(true),
            ..Default::default()
        });
        let result = sql_roundtrip(storage, &operator, &response).await;
        // Attempt cleanup on setup/SQL/assertion errors as well as on success. The root
        // always includes our UUID; never remove the configured root or the bucket root.
        let cleanup = operator.remove_all("").await;
        if let Err(error) = &cleanup {
            eprintln!("Failed to clean real S3 test prefix {root}: {error}");
        }
        result?;
        cleanup.with_context(|| format!("clean real S3 test prefix {root}"))?;
        Ok(())
    }
}

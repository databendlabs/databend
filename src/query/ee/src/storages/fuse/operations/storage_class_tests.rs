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
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    use anyhow::Context;
    use chrono::Utc;
    use databend_common_catalog::session_type::SessionType;
    use databend_common_catalog::table::Table;
    use databend_common_catalog::table_context::TableContextTableAccess;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::block_debug::pretty_format_blocks;
    use databend_common_expression::types::Int64Type;
    use databend_common_meta_app::storage::S3StorageClass;
    use databend_common_meta_app::storage::StorageParams;
    use databend_common_meta_app::storage::StorageS3Config;
    use databend_common_storages_fuse::FuseTable;
    use databend_common_storages_fuse::io::SegmentsIO;
    use databend_query::test_kits::TestFixture;
    use databend_storages_common_table_meta::meta::CompactSegmentInfo;
    use databend_storages_common_table_meta::table::OPT_KEY_S3_STORAGE_CLASS;
    use futures::TryStreamExt;
    use opendal::Buffer;
    use opendal::Operator;
    use opendal::layers::HttpClientLayer;
    use opendal::raw::HttpBody;
    use opendal::raw::HttpClient;
    use opendal::raw::HttpFetch;
    use opendal::services::S3;

    use crate::test_kits::context::EESetup;

    // OpenDAL Metadata does not expose S3 StorageClass. Observe the real HEAD
    // response on the inspection operator instead; SQL/FUSE writers are unchanged.
    // Forward requests and responses without modification, preserving normal
    // Authorization-header signing (no presigned URLs or mocked transport).
    struct HeadObserver {
        client: HttpClient,
        storage_class: Arc<Mutex<Option<String>>>,
    }

    impl HttpFetch for HeadObserver {
        async fn fetch(
            &self,
            request: http::Request<Buffer>,
        ) -> opendal::Result<http::Response<HttpBody>> {
            let is_head = request.method() == http::Method::HEAD;
            let response = self.client.fetch(request).await?;
            if is_head && response.status().is_success() {
                // Only a successful HEAD with no header means STANDARD, never an error.
                let value = response
                    .headers()
                    .get("x-amz-storage-class")
                    .map(|value| value.to_str())
                    .transpose()
                    .map_err(|e| {
                        opendal::Error::new(opendal::ErrorKind::Unexpected, e.to_string())
                    })?
                    .unwrap_or("STANDARD")
                    .to_owned();
                *self.storage_class.lock().unwrap() = Some(value);
            }
            Ok(response)
        }
    }

    async fn table_objects(fixture: &TestFixture, name: &str) -> anyhow::Result<BTreeSet<String>> {
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "storage_class", name).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table
            .read_table_snapshot_without_cache()
            .await?
            .context("INSERT must create a snapshot")?;
        let location = table.snapshot_loc().context("missing snapshot location")?;
        let mut objects = BTreeSet::from([location]);
        anyhow::ensure!(!snapshot.segments.is_empty(), "INSERT must create segments");
        let segments_io = SegmentsIO::create(ctx, table.get_operator(), table.schema());
        let segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(&snapshot.segments, false)
            .await?;
        for ((location, _), segment) in snapshot.segments.iter().zip(segments) {
            objects.insert(location.clone());
            let blocks = segment?.block_metas()?;
            anyhow::ensure!(!blocks.is_empty(), "INSERT must create blocks");
            objects.extend(blocks.iter().map(|block| block.location.0.clone()));
        }
        Ok(objects)
    }

    async fn check_objects<'a>(
        operator: &Operator,
        observed: &Mutex<Option<String>>,
        paths: impl IntoIterator<Item = &'a String>,
        expected: &str,
    ) -> anyhow::Result<usize> {
        let mut count = 0;
        for path in paths {
            *observed.lock().unwrap() = None;
            operator
                .stat(path)
                .await
                .with_context(|| format!("HeadObject {path}"))?;
            let actual = observed
                .lock()
                .unwrap()
                .take()
                .context("stat must issue a successful real HEAD request")?;
            anyhow::ensure!(
                actual == expected,
                "{path}: expected {expected}, AWS returned {actual}"
            );
            count += 1;
        }
        anyhow::ensure!(count > 0, "no objects checked");
        Ok(count)
    }

    async fn check_data(
        fixture: &TestFixture,
        table: &str,
        expected: &[i64],
    ) -> anyhow::Result<()> {
        let blocks: Vec<_> = fixture
            .execute_query(&format!("SELECT id FROM storage_class.{table} ORDER BY id"))
            .await?
            .try_collect()
            .await?;
        let expected = DataBlock::new_from_columns(vec![Int64Type::from_data(expected.to_vec())]);
        let actual = pretty_format_blocks(&blocks)?;
        let expected = pretty_format_blocks(&[expected])?;
        anyhow::ensure!(
            actual == expected,
            "{table}: expected\n{expected}\ngot\n{actual}"
        );
        Ok(())
    }

    async fn setup_sql_fixture(setup: EESetup) -> anyhow::Result<TestFixture> {
        // Dummy sessions cannot create temporary tables; keep one MySQL session.
        Ok(TestFixture::setup_with_custom_and_session_type(setup, SessionType::MySQL).await?)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sql_fixture_supports_temporary_tables() -> anyhow::Result<()> {
        // Local filesystem regression for the same session setup as the AWS test.
        let fixture = setup_sql_fixture(EESetup::new()).await?;
        for sql in [
            "CREATE DATABASE storage_class",
            "SET s3_storage_class = 'intelligent_tiering'",
            "CREATE TEMPORARY TABLE storage_class.temp(id INT) S3_STORAGE_CLASS = 'intelligent_tiering'",
            "INSERT INTO storage_class.temp VALUES (13)",
        ] {
            fixture.execute_command(sql).await?;
        }
        check_data(&fixture, "temp", &[13]).await?;
        anyhow::ensure!(
            table_objects(&fixture, "temp").await?.len() == 3,
            "expected snapshot, segment and block"
        );
        fixture
            .execute_command("DROP TABLE storage_class.temp")
            .await?;
        fixture
            .execute_command("DROP DATABASE storage_class")
            .await?;
        Ok(())
    }

    async fn sql_roundtrip(
        storage: StorageS3Config,
        operator: &Operator,
        observed: &Mutex<Option<String>>,
    ) -> anyhow::Result<()> {
        let mut setup = EESetup::new();
        setup.config_mut().storage.params = StorageParams::S3(storage.clone());
        setup.config_mut().storage.allow_insecure = false;
        let fixture = setup_sql_fixture(setup).await?;
        for sql in [
            // Keep original objects intact when checking that ALTER is not retroactive.
            "SET auto_compaction_imperfect_blocks_threshold = 0",
            "CREATE DATABASE storage_class OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'intelligent_tiering')",
            "SET s3_storage_class = 'standard'",
            "CREATE TABLE storage_class.inherited(id INT)",
            "INSERT INTO storage_class.inherited VALUES (1)",
        ] {
            fixture.execute_command(sql).await?;
        }
        let before = table_objects(&fixture, "inherited").await?;
        check_objects(operator, observed, &before, "INTELLIGENT_TIERING").await?;
        check_data(&fixture, "inherited", &[1]).await?;

        // Table STANDARD overrides both database and session defaults for new objects.
        for sql in [
            "SET s3_storage_class = 'intelligent_tiering'",
            "ALTER TABLE storage_class.inherited SET OPTIONS(S3_STORAGE_CLASS = 'standard')",
            "INSERT INTO storage_class.inherited VALUES (3)",
        ] {
            fixture.execute_command(sql).await?;
        }
        let after = table_objects(&fixture, "inherited").await?;
        let count =
            check_objects(operator, observed, after.difference(&before), "STANDARD").await?;
        anyhow::ensure!(count == 3, "expected new snapshot, segment and block");
        check_objects(operator, observed, &before, "INTELLIGENT_TIERING").await?;
        check_data(&fixture, "inherited", &[1, 3]).await?;

        // Temporary tables force STANDARD even with an explicit INTELLIGENT_TIERING.
        for sql in [
            "CREATE TEMPORARY TABLE storage_class.temp(id INT) S3_STORAGE_CLASS = 'intelligent_tiering'",
            "INSERT INTO storage_class.temp VALUES (13)",
        ] {
            fixture.execute_command(sql).await?;
        }
        check_objects(
            operator,
            observed,
            &table_objects(&fixture, "temp").await?,
            "STANDARD",
        )
        .await?;
        check_data(&fixture, "temp", &[13]).await?;
        fixture
            .execute_command("DROP TABLE storage_class.temp")
            .await?;

        // External operators use explicit runtime params; ambient IAM is not persisted
        // for external SQL tables. Use a distinct root to catch internal-root fallback.
        let mut external_storage = storage;
        external_storage.root =
            format!("{}/external/", external_storage.root.trim_end_matches('/'));
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx
            .get_table("default", "storage_class", "inherited")
            .await?;
        let mut info = table.get_table_info().clone();
        info.meta.storage_params = Some(StorageParams::S3(external_storage));
        info.meta.options.insert(
            OPT_KEY_S3_STORAGE_CLASS.to_owned(),
            "intelligent_tiering".to_owned(),
        );
        let external = FuseTable::create_storage_operator(&info, Some(S3StorageClass::Standard))?;
        external
            .write("storage-class-probe", "external data")
            .await?;
        anyhow::ensure!(
            external.read("storage-class-probe").await?.to_vec() == b"external data",
            "external operator data readback mismatch"
        );
        check_objects(
            operator,
            observed,
            &["external/storage-class-probe".to_owned()],
            "INTELLIGENT_TIERING",
        )
        .await?;
        fixture
            .execute_command("DROP DATABASE storage_class")
            .await?;
        Ok(())
    }

    /// SQL -> FUSE -> real AWS S3, plus an external-operator probe.
    /// Requires DATABEND_TEST_S3_BUCKET/REGION, optional DATABEND_TEST_S3_ROOT,
    /// and runner credentials with list/get/put/delete access to the test prefix.
    /// Run: cargo test -p databend-enterprise-query --lib real_s3::storage_class_sql_roundtrip -- --ignored
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
        let test_id = databend_storages_common_table_meta::meta::uuid_from_date_time(Utc::now())
            .simple()
            .to_string();
        let root = format!(
            "{}/storage-class/{test_id}/",
            configured_root.trim_matches('/')
        );
        let endpoint = format!("https://s3.{region}.amazonaws.com");
        let observed = Arc::new(Mutex::new(None));
        let operator = Operator::new(
            S3::default()
                .bucket(&bucket)
                .region(&region)
                .endpoint(&endpoint)
                .root(&root),
        )?
        .finish()
        .layer(HttpClientLayer::new(HttpClient::with(HeadObserver {
            client: HttpClient::new()?,
            storage_class: observed.clone(),
        })));
        let storage = StorageS3Config {
            bucket,
            region,
            root: root.clone(),
            endpoint_url: endpoint,
            allow_credential_chain: Some(true),
            ..Default::default()
        };
        let result = sql_roundtrip(storage, &operator, &observed).await;
        // This operator is rooted at our UUID, never the configured or bucket root.
        // Attempt cleanup after setup/SQL/assertion errors as well as on success.
        let cleanup = operator.remove_all("").await;
        if let Err(error) = &cleanup {
            eprintln!("Failed to clean real S3 test prefix {root}: {error}");
        }
        result?;
        cleanup.with_context(|| format!("clean real S3 test prefix {root}"))?;
        Ok(())
    }
}

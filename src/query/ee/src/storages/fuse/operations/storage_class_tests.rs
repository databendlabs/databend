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
    use databend_common_meta_app::principal::UserDefinedConnection;
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_meta_app::storage::S3StorageClass;
    use databend_common_meta_app::storage::StorageParams;
    use databend_common_meta_app::storage::StorageS3Config;
    use databend_common_sql::binder::EXTERNAL_STORAGE_VERIFICATION_KEY;
    use databend_common_storages_fuse::FuseTable;
    use databend_common_storages_fuse::io::SegmentsIO;
    use databend_common_users::UserApiProvider;
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
    use reqsign::AwsConfig;
    use reqsign::AwsDefaultLoader;

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

    async fn table_objects(
        fixture: &TestFixture,
        name: &str,
        prefix: &str,
    ) -> anyhow::Result<BTreeSet<String>> {
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "storage_class", name).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table
            .read_table_snapshot_without_cache()
            .await?
            .context("INSERT must create a snapshot")?;
        let location = table.snapshot_loc().context("missing snapshot location")?;
        let mut objects = BTreeSet::from([format!("{prefix}{location}")]);
        anyhow::ensure!(!snapshot.segments.is_empty(), "INSERT must create segments");
        let segments_io = SegmentsIO::create(ctx, table.get_operator(), table.schema());
        let segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(&snapshot.segments, false)
            .await?;
        for ((location, _), segment) in snapshot.segments.iter().zip(segments) {
            objects.insert(format!("{prefix}{location}"));
            let blocks = segment?.block_metas()?;
            anyhow::ensure!(!blocks.is_empty(), "INSERT must create blocks");
            objects.extend(
                blocks
                    .iter()
                    .map(|block| format!("{prefix}{}", block.location.0)),
            );
        }
        Ok(objects)
    }

    async fn check_objects<P: AsRef<str>>(
        operator: &Operator,
        observed: &Mutex<Option<String>>,
        paths: impl IntoIterator<Item = P>,
        expected: &str,
    ) -> anyhow::Result<()> {
        let mut count = 0;
        for path in paths {
            let path = path.as_ref();
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
        Ok(())
    }

    async fn check_probe(
        operator: &Operator,
        observed: &Mutex<Option<String>>,
        prefix: &str,
        expected: &str,
    ) -> anyhow::Result<()> {
        check_objects(
            operator,
            observed,
            [format!("{prefix}{EXTERNAL_STORAGE_VERIFICATION_KEY}")],
            expected,
        )
        .await
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

    async fn sql_roundtrip(
        storage: StorageS3Config,
        operator: &Operator,
        observed: &Mutex<Option<String>>,
    ) -> anyhow::Result<()> {
        let mut setup = EESetup::new();
        setup.config_mut().storage.params = StorageParams::S3(storage.clone());
        setup.config_mut().storage.allow_insecure = false;
        // Dummy sessions cannot create temporary tables; keep one MySQL session.
        let fixture =
            TestFixture::setup_with_custom_and_session_type(setup, SessionType::MySQL).await?;
        let execute = async |sql: &str| fixture.execute_command(sql).await;
        for sql in [
            "CREATE DATABASE storage_class OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'intelligent_tiering')",
            "SET experimental_s3_storage_class = 'intelligent_tiering'",
            "SET auto_compaction_imperfect_blocks_threshold = 0",
            "CREATE TABLE storage_class.internal(id INT)",
            "INSERT INTO storage_class.internal VALUES (1)",
        ] {
            execute(sql).await?;
        }
        check_objects(
            operator,
            observed,
            &table_objects(&fixture, "internal", "").await?,
            "STANDARD",
        )
        .await?;
        check_data(&fixture, "internal", &[1]).await?;
        execute("DROP DATABASE storage_class").await?;

        // Reuse OpenDAL's loader. Register the credentials through the test API,
        // never SQL text/history; do not enable ambient IAM for external SQL locations.
        let mut aws_config = AwsConfig::default().from_profile().from_env();
        aws_config.region = Some(storage.region.clone());
        let credentials = AwsDefaultLoader::new(reqwest::Client::new(), aws_config)
            .load()
            .await?
            .context("AWS runner credentials are required")?;
        let mut params = BTreeMap::from([
            ("endpoint_url".to_owned(), storage.endpoint_url.clone()),
            ("region".to_owned(), storage.region.clone()),
            ("access_key_id".to_owned(), credentials.access_key_id),
            (
                "secret_access_key".to_owned(),
                credentials.secret_access_key,
            ),
        ]);
        if let Some(token) = credentials.session_token {
            params.insert("security_token".to_owned(), token);
        }
        UserApiProvider::instance()
            .add_connection(
                &fixture.default_tenant(),
                UserDefinedConnection::new("storage_class_s3", "s3".to_owned(), params),
                &CreateOption::Create,
            )
            .await?;
        let base = format!(
            "s3://{}/{}/",
            storage.bucket,
            storage.root.trim_matches('/')
        );

        // CREATE DATABASE must probe its explicit class, not the STANDARD session.
        execute("SET experimental_s3_storage_class = 'standard'").await?;
        execute(&format!("CREATE DATABASE storage_class OPTIONS(DEFAULT_STORAGE_CONNECTION = 'storage_class_s3', DEFAULT_STORAGE_PATH = '{base}external/', DEFAULT_S3_STORAGE_CLASS = 'intelligent_tiering')")).await?;
        check_probe(operator, observed, "external/", "INTELLIGENT_TIERING").await?;

        // CREATE TABLE probes and data writes agree for implicit/explicit locations.
        for (name, ddl, session, prefix, expected) in [
            (
                "inherited",
                "CREATE TABLE storage_class.inherited(id INT)".to_owned(),
                "standard",
                "external/",
                "INTELLIGENT_TIERING",
            ),
            (
                "explicit",
                format!(
                    "CREATE TABLE storage_class.explicit(id INT) '{base}explicit/' CONNECTION = (connection_name = 'storage_class_s3') S3_STORAGE_CLASS = 'standard'"
                ),
                "intelligent_tiering",
                "explicit/",
                "STANDARD",
            ),
            (
                "uri_default",
                format!(
                    "CREATE TABLE storage_class.uri_default(id INT) '{base}uri-default/' CONNECTION = (connection_name = 'storage_class_s3')"
                ),
                "standard",
                "uri-default/",
                "INTELLIGENT_TIERING",
            ),
        ] {
            execute(&format!("SET experimental_s3_storage_class = '{session}'")).await?;
            execute(&ddl).await?;
            check_probe(operator, observed, prefix, expected).await?;
            execute(&format!("INSERT INTO storage_class.{name} VALUES (1)")).await?;
            check_objects(
                operator,
                observed,
                table_objects(&fixture, name, prefix).await?,
                expected,
            )
            .await?;
            check_data(&fixture, name, &[1]).await?;
        }

        // SET/UNSET affect future objects only, with UNSET falling back to the session.
        let before = table_objects(&fixture, "inherited", "external/").await?;
        execute("ALTER TABLE storage_class.inherited SET OPTIONS(S3_STORAGE_CLASS = 'standard')")
            .await?;
        execute("INSERT INTO storage_class.inherited VALUES (3)").await?;
        let after = table_objects(&fixture, "inherited", "external/").await?;
        check_objects(operator, observed, after.difference(&before), "STANDARD").await?;
        check_objects(operator, observed, &before, "INTELLIGENT_TIERING").await?;
        execute("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
        execute("ALTER TABLE storage_class.inherited UNSET OPTIONS(S3_STORAGE_CLASS)").await?;
        execute("INSERT INTO storage_class.inherited VALUES (5)").await?;
        let unset = table_objects(&fixture, "inherited", "external/").await?;
        check_objects(
            operator,
            observed,
            unset.difference(&after),
            "INTELLIGENT_TIERING",
        )
        .await?;
        check_data(&fixture, "inherited", &[1, 3, 5]).await?;

        // Reject explicit temporary classes before probing either kind of location.
        let invalid_uri = format!(
            "CREATE TEMPORARY TABLE storage_class.invalid_uri(id INT) '{base}temp/' CONNECTION = (connection_name = 'storage_class_s3') S3_STORAGE_CLASS = 'standard'"
        );
        for sql in [
            "CREATE TEMPORARY TABLE storage_class.invalid_temp(id INT) S3_STORAGE_CLASS = 'intelligent_tiering'",
            &invalid_uri,
        ] {
            let Err(error) = execute(sql).await else {
                anyhow::bail!("temporary table accepted a storage class")
            };
            anyhow::ensure!(
                error.code() == databend_common_exception::ErrorCode::TABLE_OPTION_INVALID,
                "unexpected error: {error}"
            );
        }
        for (ddl, prefix) in [
            (
                "CREATE TEMPORARY TABLE storage_class.temp(id INT)".to_owned(),
                "external/",
            ),
            (
                format!(
                    "CREATE TEMPORARY TABLE storage_class.temp(id INT) '{base}temp/' CONNECTION = (connection_name = 'storage_class_s3')"
                ),
                "temp/",
            ),
        ] {
            execute(&ddl).await?;
            check_probe(operator, observed, prefix, "STANDARD").await?;
            execute("INSERT INTO storage_class.temp VALUES (13)").await?;
            check_objects(
                operator,
                observed,
                table_objects(&fixture, "temp", prefix).await?,
                "STANDARD",
            )
            .await?;
            check_data(&fixture, "temp", &[13]).await?;
            let Err(error) = execute(
                "ALTER TABLE storage_class.temp SET OPTIONS(S3_STORAGE_CLASS = 'standard')",
            )
            .await
            else {
                anyhow::bail!("temporary SET accepted a storage class")
            };
            anyhow::ensure!(
                error.code() == databend_common_exception::ErrorCode::TABLE_OPTION_INVALID,
                "unexpected SET error: {error}"
            );
            execute("ALTER TABLE storage_class.temp UNSET OPTIONS(S3_STORAGE_CLASS)").await?;
            execute("DROP TABLE storage_class.temp").await?;
        }

        // Partial ALTER probes merged defaults, not just fields in the ALTER statement.
        execute("SET experimental_s3_storage_class = 'standard'").await?;
        execute(&format!(
            "ALTER DATABASE storage_class SET OPTIONS(DEFAULT_STORAGE_PATH = '{base}changed/')"
        ))
        .await?;
        check_probe(operator, observed, "changed/", "INTELLIGENT_TIERING").await?;
        execute("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
        execute("ALTER DATABASE storage_class SET OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'standard')")
            .await?;
        check_probe(operator, observed, "changed/", "STANDARD").await?;
        execute("CREATE TABLE storage_class.after_alter(id INT)").await?;
        execute("INSERT INTO storage_class.after_alter VALUES (1)").await?;
        check_objects(
            operator,
            observed,
            table_objects(&fixture, "after_alter", "changed/").await?,
            "STANDARD",
        )
        .await?;
        // Existing tables keep the old inherited class even for subsequent writes.
        execute("SET experimental_s3_storage_class = 'standard'").await?;
        execute("INSERT INTO storage_class.uri_default VALUES (2)").await?;
        check_objects(
            operator,
            observed,
            table_objects(&fixture, "uri_default", "uri-default/").await?,
            "INTELLIGENT_TIERING",
        )
        .await?;
        check_data(&fixture, "uri_default", &[1, 2]).await?;
        execute("DROP DATABASE storage_class").await?;

        // Session fallback affects probes without being persisted in metadata.
        execute("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
        execute(&format!("CREATE DATABASE storage_class OPTIONS(DEFAULT_STORAGE_CONNECTION = 'storage_class_s3', DEFAULT_STORAGE_PATH = '{base}session/')")).await?;
        check_probe(operator, observed, "session/", "INTELLIGENT_TIERING").await?;
        execute("CREATE TABLE storage_class.session_default(id INT)").await?;
        check_probe(operator, observed, "session/", "INTELLIGENT_TIERING").await?;
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx
            .get_table("default", "storage_class", "session_default")
            .await?;
        anyhow::ensure!(
            !table.options().contains_key(OPT_KEY_S3_STORAGE_CLASS),
            "session class was persisted as a table option"
        );
        anyhow::ensure!(
            matches!(&table.get_table_info().meta.storage_params,
            Some(StorageParams::S3(params)) if params.storage_class == S3StorageClass::Standard),
            "session class was persisted in storage parameters"
        );
        execute("DROP DATABASE storage_class").await?;
        execute("DROP CONNECTION storage_class_s3").await?;
        Ok(())
    }

    /// Real S3 DDL probes and table writes use the effective class; internal SQL ignores it.
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

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

    // OpenDAL Metadata does not expose S3 StorageClass. Observe successful real
    // HEAD responses on the inspector, not SQL/FUSE writers. Forward requests and
    // responses unchanged, preserving Authorization-header signing (no presign/mock).
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
                // Only a successful HEAD without the header means STANDARD, never an error.
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

    struct AwsCase {
        fixture: TestFixture,
        inspector: Operator,
        observed: Arc<Mutex<Option<String>>>,
        base_uri: String,
    }

    impl AwsCase {
        async fn sql(&self, sql: &str) -> anyhow::Result<()> {
            self.fixture
                .execute_command(sql)
                .await
                .with_context(|| format!("SQL: {sql}"))
        }

        fn uri(&self, prefix: &str) -> String {
            format!("{}{prefix}", self.base_uri)
        }

        // All database-provided external locations start at this case's external/ prefix.
        async fn create_database(&self, class: Option<&str>) -> anyhow::Result<()> {
            let class = class
                .map(|c| format!(", DEFAULT_S3_STORAGE_CLASS = '{c}'"))
                .unwrap_or_default();
            self.sql(&format!("CREATE DATABASE storage_class OPTIONS(DEFAULT_STORAGE_CONNECTION = 'storage_class_s3', DEFAULT_STORAGE_PATH = '{}'{class})", self.uri("external/"))).await
        }

        async fn rejects_class_override(&self, sql: &str) -> anyhow::Result<()> {
            let Err(error) = self.fixture.execute_command(sql).await else {
                anyhow::bail!("expected storage-class rejection: {sql}");
            };
            anyhow::ensure!(
                error.code() == databend_common_exception::ErrorCode::TABLE_OPTION_INVALID,
                "unexpected error for {sql}: {error}"
            );
            Ok(())
        }

        async fn objects<P: AsRef<str>>(
            &self,
            paths: impl IntoIterator<Item = P>,
            expected: &str,
        ) -> anyhow::Result<()> {
            let mut count = 0;
            for path in paths {
                let path = path.as_ref();
                *self.observed.lock().unwrap() = None;
                self.inspector
                    .stat(path)
                    .await
                    .with_context(|| format!("HeadObject {path}"))?;
                let actual = self
                    .observed
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

        async fn probe(&self, prefix: &str, expected: &str) -> anyhow::Result<()> {
            self.objects(
                [format!("{prefix}{EXTERNAL_STORAGE_VERIFICATION_KEY}")],
                expected,
            )
            .await
        }

        // Return referenced immutable objects, excluding mutable hints/probe files.
        async fn table_objects(
            &self,
            name: &str,
            prefix: &str,
        ) -> anyhow::Result<BTreeSet<String>> {
            let ctx = self.fixture.new_query_ctx().await?;
            let table = ctx.get_table("default", "storage_class", name).await?;
            let table = FuseTable::try_from_table(table.as_ref())?;
            let snapshot = table
                .read_table_snapshot_without_cache()
                .await?
                .context("missing snapshot")?;
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
                objects.extend(blocks.iter().map(|b| format!("{prefix}{}", b.location.0)));
            }
            Ok(objects)
        }

        async fn table(&self, name: &str, prefix: &str, expected: &str) -> anyhow::Result<()> {
            self.objects(self.table_objects(name, prefix).await?, expected)
                .await
        }
    }

    // Every test gets its own metadata fixture and UUID-scoped S3 root. Keep setup
    // inside the result block so setup/SQL/assertion errors all attempt cleanup.
    // AWS tests are opt-in: set DATABEND_TEST_S3_BUCKET/REGION and optional ROOT;
    // run with nextest's storage_class_tests::real_s3:: filter and --run-ignored only.
    async fn run_case(
        name: &str,
        test: impl AsyncFnOnce(&AwsCase) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let bucket = std::env::var("DATABEND_TEST_S3_BUCKET")?;
        let region = std::env::var("DATABEND_TEST_S3_REGION")?;
        anyhow::ensure!(
            !bucket.is_empty() && !region.is_empty(),
            "bucket and region must not be empty"
        );
        let configured_root = std::env::var("DATABEND_TEST_S3_ROOT").unwrap_or_default();
        let id = databend_storages_common_table_meta::meta::uuid_from_date_time(Utc::now())
            .simple()
            .to_string();
        let root = format!(
            "{}/storage-class/{name}/{id}/",
            configured_root.trim_matches('/')
        );
        let endpoint = format!("https://s3.{region}.amazonaws.com");
        let observed = Arc::new(Mutex::new(None));
        let inspector = Operator::new(
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
        let result = async {
            let mut setup = EESetup::new();
            setup.config_mut().storage.params = StorageParams::S3(StorageS3Config {
                bucket: bucket.clone(),
                region: region.clone(),
                root: root.clone(),
                endpoint_url: endpoint.clone(),
                allow_credential_chain: Some(true),
                ..Default::default()
            });
            // Dummy sessions cannot create temporary tables.
            let fixture =
                TestFixture::setup_with_custom_and_session_type(setup, SessionType::MySQL).await?;
            // Use the existing loader and test API, never credential-bearing SQL or
            // ambient IAM access for user-controlled external SQL locations.
            let mut config = AwsConfig::default().from_profile().from_env();
            config.region = Some(region.clone());
            let credentials = AwsDefaultLoader::new(reqwest::Client::new(), config)
                .load()
                .await?
                .context("AWS runner credentials are required")?;
            let mut params = BTreeMap::from([
                ("endpoint_url".to_owned(), endpoint),
                ("region".to_owned(), region),
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
            let case = AwsCase {
                fixture,
                inspector: inspector.clone(),
                observed,
                base_uri: format!("s3://{}/{}/", bucket, root.trim_matches('/')),
            };
            test(&case).await
        }
        .await;
        // Never remove the shared configured/bucket root. Test metadata is isolated
        // with the fixture; S3 objects need explicit cleanup, including error paths.
        let cleanup = inspector.remove_all("").await;
        if let Err(error) = &cleanup {
            eprintln!("Failed to clean real S3 test prefix {root}: {error}");
        }
        result.with_context(|| format!("AWS storage-class case: {name}"))?;
        cleanup.with_context(|| format!("clean real S3 test prefix {root}"))?;
        Ok(())
    }

    /// A class-only database default and an IT session must not move internal data
    /// out of the server-configured STANDARD class (the operator owns this bucket).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn internal_table_ignores_user_storage_classes() -> anyhow::Result<()> {
        run_case("internal", async |c| {
            c.sql("CREATE DATABASE storage_class OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'intelligent_tiering')").await?;
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
            c.sql("CREATE TABLE storage_class.internal(id INT)").await?;
            c.sql("INSERT INTO storage_class.internal VALUES (1)").await?;
            c.table("internal", "", "STANDARD").await
        }).await
    }

    /// DB IT beats a STANDARD session for CREATE/path-only ALTER probes. A class-only
    /// ALTER to STANDARD beats an IT session; existing tables keep their old default.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn database_probe_uses_default_and_merged_class() -> anyhow::Result<()> {
        run_case("database", async |c| {
            c.sql("SET experimental_s3_storage_class = 'standard'")
                .await?;
            c.create_database(Some("intelligent_tiering")).await?;
            c.probe("external/", "INTELLIGENT_TIERING").await?;
            c.sql("CREATE TABLE storage_class.before_alter(id INT)")
                .await?;
            c.sql(&format!(
                "ALTER DATABASE storage_class SET OPTIONS(DEFAULT_STORAGE_PATH = '{}')",
                c.uri("changed/")
            ))
            .await?;
            c.probe("changed/", "INTELLIGENT_TIERING").await?;
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'")
                .await?;
            c.sql(
                "ALTER DATABASE storage_class SET OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'standard')",
            )
            .await?;
            c.probe("changed/", "STANDARD").await?;
            c.sql("CREATE TABLE storage_class.after_alter(id INT)")
                .await?;
            c.sql("INSERT INTO storage_class.after_alter VALUES (1)")
                .await?;
            c.table("after_alter", "changed/", "STANDARD").await?;
            c.sql("SET experimental_s3_storage_class = 'standard'")
                .await?;
            c.sql("INSERT INTO storage_class.before_alter VALUES (1)")
                .await?;
            c.table("before_alter", "external/", "INTELLIGENT_TIERING")
                .await
        })
        .await
    }

    /// Inheritance applies to both a DB-provided location and an explicit URI.
    /// Probe objects and all referenced table objects must use DB IT, not session STANDARD.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn table_probe_inherits_database_class_for_both_locations() -> anyhow::Result<()> {
        run_case("inheritance", async |c| {
            c.sql("SET experimental_s3_storage_class = 'standard'")
                .await?;
            c.create_database(Some("intelligent_tiering")).await?;
            for (name, location, prefix) in [
                ("implicit", String::new(), "external/"),
                (
                    "explicit",
                    format!(
                        " '{}' CONNECTION = (connection_name = 'storage_class_s3')",
                        c.uri("explicit/")
                    ),
                    "explicit/",
                ),
            ] {
                c.sql(&format!(
                    "CREATE TABLE storage_class.{name}(id INT){location}"
                ))
                .await?;
                c.probe(prefix, "INTELLIGENT_TIERING").await?;
                c.sql(&format!("INSERT INTO storage_class.{name} VALUES (1)"))
                    .await?;
                c.table(name, prefix, "INTELLIGENT_TIERING").await?;
            }
            Ok(())
        })
        .await
    }

    /// An explicit table STANDARD must beat both DB IT and session IT, for the
    /// permission probe as well as the actual block/segment/snapshot PUTs.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn table_probe_prefers_explicit_class() -> anyhow::Result<()> {
        run_case("override", async |c| {
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
            c.create_database(Some("intelligent_tiering")).await?;
            c.sql(&format!("CREATE TABLE storage_class.explicit(id INT) '{}' CONNECTION = (connection_name = 'storage_class_s3') S3_STORAGE_CLASS = 'standard'", c.uri("explicit/"))).await?;
            c.probe("explicit/", "STANDARD").await?;
            c.sql("INSERT INTO storage_class.explicit VALUES (1)").await?;
            c.table("explicit", "explicit/", "STANDARD").await
        }).await
    }

    /// SET changes only new external objects. With DB STANDARD and session IT,
    /// UNSET must use the session, not re-inherit the database default. Old IT objects stay IT.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn table_set_unset_changes_only_new_objects() -> anyhow::Result<()> {
        run_case("set-unset", async |c| {
            // Avoid compaction rewriting the old objects whose class we recheck.
            c.sql("SET auto_compaction_imperfect_blocks_threshold = 0")
                .await?;
            c.create_database(Some("intelligent_tiering")).await?;
            c.sql("CREATE TABLE storage_class.t(id INT)").await?;
            c.sql("INSERT INTO storage_class.t VALUES (1)").await?;
            let before = c.table_objects("t", "external/").await?;
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'")
                .await?;
            c.sql("ALTER TABLE storage_class.t SET OPTIONS(S3_STORAGE_CLASS = 'standard')")
                .await?;
            c.sql("INSERT INTO storage_class.t VALUES (3)").await?;
            let after = c.table_objects("t", "external/").await?;
            c.objects(after.difference(&before), "STANDARD").await?;
            c.objects(&before, "INTELLIGENT_TIERING").await?;
            c.sql(
                "ALTER DATABASE storage_class SET OPTIONS(DEFAULT_S3_STORAGE_CLASS = 'standard')",
            )
            .await?;
            c.sql("ALTER TABLE storage_class.t UNSET OPTIONS(S3_STORAGE_CLASS)")
                .await?;
            c.sql("INSERT INTO storage_class.t VALUES (5)").await?;
            let unset = c.table_objects("t", "external/").await?;
            c.objects(unset.difference(&after), "INTELLIGENT_TIERING")
                .await
        })
        .await
    }

    /// Even with DB/session IT, temporary external tables must probe/write STANDARD.
    /// Both explicit and inherited locations reject CREATE/SET class overrides.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn temporary_external_tables_use_standard_and_reject_overrides() -> anyhow::Result<()> {
        run_case("temporary", async |c| {
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'")
                .await?;
            c.create_database(Some("intelligent_tiering")).await?;
            for (location, prefix) in [
                (String::new(), "external/"),
                (
                    format!(
                        " '{}' CONNECTION = (connection_name = 'storage_class_s3')",
                        c.uri("temp/")
                    ),
                    "temp/",
                ),
            ] {
                let ddl = format!("CREATE TEMPORARY TABLE storage_class.temp(id INT){location}");
                for class in ["standard", "intelligent_tiering"] {
                    c.rejects_class_override(&format!("{ddl} S3_STORAGE_CLASS = '{class}'"))
                        .await?;
                }
                c.sql(&ddl).await?;
                c.probe(prefix, "STANDARD").await?;
                c.sql("INSERT INTO storage_class.temp VALUES (13)").await?;
                c.table("temp", prefix, "STANDARD").await?;
                c.rejects_class_override(
                    "ALTER TABLE storage_class.temp SET OPTIONS(S3_STORAGE_CLASS = 'standard')",
                )
                .await?;
                c.sql("ALTER TABLE storage_class.temp UNSET OPTIONS(S3_STORAGE_CLASS)")
                    .await?;
                c.sql("DROP TABLE storage_class.temp").await?;
            }
            Ok(())
        })
        .await
    }

    /// Without a DB/table override, IT session affects both probes, but neither the
    /// table option nor persisted storage parameters may capture that session choice.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires an explicitly configured real AWS S3 bucket"]
    async fn session_default_affects_probes_without_persisting_override() -> anyhow::Result<()> {
        run_case("session", async |c| {
            c.sql("SET experimental_s3_storage_class = 'intelligent_tiering'").await?;
            c.create_database(None).await?;
            c.probe("external/", "INTELLIGENT_TIERING").await?;
            c.sql("CREATE TABLE storage_class.t(id INT)").await?;
            c.probe("external/", "INTELLIGENT_TIERING").await?;
            let ctx = c.fixture.new_query_ctx().await?;
            let table = ctx.get_table("default", "storage_class", "t").await?;
            anyhow::ensure!(!table.options().contains_key(OPT_KEY_S3_STORAGE_CLASS), "session class was persisted as a table option");
            anyhow::ensure!(matches!(&table.get_table_info().meta.storage_params,
                Some(StorageParams::S3(params)) if params.storage_class == S3StorageClass::Standard),
                "session class was persisted in storage parameters");
            Ok(())
        }).await
    }
}

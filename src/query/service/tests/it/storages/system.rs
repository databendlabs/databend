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

use std::io::Write;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::block_debug::box_render;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::AuthType;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateOption::Create;
use databend_common_meta_app::schema::CreateOption::CreateIfNotExists;
use databend_common_meta_app::schema::CreateOption::CreateOrReplace;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE;
use databend_common_storages_information_schema::CharacterSetsTable;
use databend_common_storages_system::BuildOptionsTable;
use databend_common_storages_system::CachesTable;
use databend_common_storages_system::CatalogsTable;
use databend_common_storages_system::ClustersTable;
use databend_common_storages_system::ColumnsTable;
use databend_common_storages_system::ConfigsTable;
use databend_common_storages_system::ContributorsTable;
use databend_common_storages_system::CreditsTable;
use databend_common_storages_system::DatabasesTableWithHistory;
use databend_common_storages_system::DatabasesTableWithoutHistory;
use databend_common_storages_system::EnginesTable;
use databend_common_storages_system::FunctionsTable;
use databend_common_storages_system::MetricsTable;
use databend_common_storages_system::RolesTable;
use databend_common_storages_system::UsersTable;
use databend_common_users::UserApiProvider;
use databend_common_version::DATABEND_BUILD_PROFILE;
use databend_common_version::DATABEND_CARGO_CFG_TARGET_FEATURE;
use databend_common_version::DATABEND_COMMIT_AUTHORS;
use databend_common_version::DATABEND_CREDITS_LICENSES;
use databend_common_version::DATABEND_CREDITS_NAMES;
use databend_common_version::DATABEND_CREDITS_VERSIONS;
use databend_common_version::DATABEND_OPT_LEVEL;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::stream::ReadDataBlockStream;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use databend_query::test_kits::execute_query;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use futures::TryStreamExt;
use goldenfile::Mint;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::any;
use wiremock::matchers::method;
use wiremock::matchers::path;

async fn run_table_tests(
    file: &mut impl Write,
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
) -> Result<()> {
    let table_info = table.get_table_info();
    writeln!(file, "---------- TABLE INFO ------------").unwrap();
    writeln!(file, "{table_info}").unwrap();
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let formatted = pretty_format_blocks(&blocks).unwrap();
    let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

    // sort except for header + footer
    let num_lines = actual_lines.len();
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }
    writeln!(file, "-------- TABLE CONTENTS ----------").unwrap();
    if table_info.name.to_lowercase() == "settings" {
        actual_lines.retain(|&item| {
            !(item.contains("max_threads")
                || item.contains("max_memory_usage")
                || item.contains("max_storage_io_requests")
                || item.contains("recluster_block_size"))
        });
    }
    for line in actual_lines {
        writeln!(file, "{}", line).unwrap();
    }
    write!(file, "\n\n").unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_build_options_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = BuildOptionsTable::create(
        1,
        option_env!("DATABEND_QUERY_CARGO_FEATURES"),
        DATABEND_CARGO_CFG_TARGET_FEATURE,
        DATABEND_BUILD_PROFILE,
        DATABEND_OPT_LEVEL,
    );
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);
    assert!(block.num_rows() >= 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_columns_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("columns_table.txt").unwrap();
    let table = ColumnsTable::create(1, "default");
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[test]
fn test_information_schema_character_sets_table_metadata() -> anyhow::Result<()> {
    let table = CharacterSetsTable::create(1, "default");
    let table_info = table.get_table_info();
    assert_eq!(table_info.name, "character_sets");
    let query = table_info
        .meta
        .options
        .get(QUERY)
        .expect("view query must be set");
    assert!(query.contains("utf8mb4"));
    assert!(query.contains("character_set_name"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clusters_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = ClustersTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configs_table_basic() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(8)?;

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("configs_table_basic.txt").unwrap();

    let table = ConfigsTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configs_table_redact() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let _file = &mut mint.new_goldenfile("configs_table_redact.txt").unwrap();

    let mock_server = MockServer::builder().start().await;
    Mock::given(method("HEAD"))
        .and(path("/test/.opendal"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = databend_query::test_kits::ConfigBuilder::create().build();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "test".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        ..Default::default()
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(8)?;

    let table = ConfigsTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    // need a method to skip/edit endpoint_url
    // run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_contributors_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = ContributorsTable::create(1, DATABEND_COMMIT_AUTHORS);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_credits_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = CreditsTable::create(
        1,
        DATABEND_CREDITS_NAMES,
        DATABEND_CREDITS_VERSIONS,
        DATABEND_CREDITS_LICENSES,
    );
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_catalogs_table() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("catalogs_table.txt").unwrap();

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = CatalogsTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_databases_table() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = DatabasesTableWithoutHistory::create(1, "default");

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("databases_table.txt").unwrap();
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_databases_history_table() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = DatabasesTableWithHistory::create(1, "default");

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint
        .new_goldenfile("databases_with_history_table.txt")
        .unwrap();
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_engines_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = EnginesTable::create(1);
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("engines_table.txt").unwrap();
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_functions_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = FunctionsTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = MetricsTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;
    let counter1 =
        databend_common_base::runtime::metrics::register_counter("test_metrics_table_count");
    let histogram1 = databend_common_base::runtime::metrics::register_histogram_in_milliseconds(
        "test_metrics_table_histogram",
    );

    counter1.inc();
    histogram1.observe(2.0);

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);
    assert!(block.num_rows() >= 1);

    let output = box_render(
        &Arc::new(source_plan.output_schema.into()),
        result.as_slice(),
        1000,
        1024,
        30,
        true,
    )?;
    assert!(output.contains("test_metrics_table_count"));
    assert!(output.contains("test_metrics_table_histogram"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_roles_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let tenant = ctx.get_tenant();

    {
        let role_info = RoleInfo::new("test", None);
        UserApiProvider::instance()
            .create_role(&tenant, role_info, &CreateOrReplace)
            .await?;
    }

    {
        let mut role_info = RoleInfo::new("test1", None);
        role_info.grants.grant_role("test".to_string());
        UserApiProvider::instance()
            .create_role(&tenant, role_info, &Create)
            .await?;
    }

    {
        let mut role_info = RoleInfo::new("test1", None);
        role_info.grants.grant_role("t2".to_string());
        UserApiProvider::instance()
            .create_role(&tenant, role_info, &CreateIfNotExists)
            .await?;
    }

    let table = RolesTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;
    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);
    assert_eq!(block.num_rows(), 4);

    let output = box_render(
        &Arc::new(source_plan.output_schema.into()),
        result.as_slice(),
        1000,
        1024,
        30,
        true,
    )?;
    assert!(output.contains("test"));
    assert!(output.contains("test1"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_users_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let tenant = ctx.get_tenant();
    let auth_data = AuthInfo::None;
    let user_info = UserInfo::new("test", "%", auth_data);
    UserApiProvider::instance()
        .create_user(&tenant, user_info, &CreateOption::Create)
        .await?;
    let auth_data = AuthInfo::new(
        AuthType::Sha256Password,
        &Some("123456789".to_string()),
        false,
    );
    assert!(auth_data.is_ok());
    let mut user_info = UserInfo::new("test1", "%", auth_data?);
    user_info.option.set_default_role(Some("role1".to_string()));
    UserApiProvider::instance()
        .create_user(&tenant, user_info, &CreateOption::Create)
        .await?;

    let table = UsersTable::create(1);
    let source_plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;
    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 14);
    assert!(block.num_rows() >= 2);

    let output = box_render(
        &Arc::new(source_plan.output_schema.into()),
        result.as_slice(),
        1000,
        1024,
        30,
        true,
    )?;
    assert!(output.contains("test"));
    assert!(output.contains("test1"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caches_table() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("caches_table.txt").unwrap();

    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;
    let cluster_desc = ClusterDescriptor::new().with_local_id("test-node");
    let ctx = fixture.new_query_ctx_with_cluster(cluster_desc).await?;

    let table = CachesTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_tables_ignores_broken_attached_table_refresh() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let catalog = ctx.get_catalog("default").await?;
    let database = catalog.get_database(&tenant, "default").await?;

    execute_command(ctx.clone(), "create table default.healthy(a int)").await?;

    // Simulate a broken attached-table storage: any attempt to refresh from this
    // S3 endpoint will fail with 403, matching the original issue's behavior.
    let mock_server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let broken_schema = Arc::new(TableSchema::new(vec![TableField::new(
        "a",
        TableDataType::Number(NumberDataType::Int32),
    )]));

    // Register a FUSE attached table whose refresh path points at the failing mock
    // endpoint above. Without disable_table_info_refresh, loading this table will
    // try to fetch the last snapshot hint and fail.
    catalog
        .create_table(CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: tenant.clone(),
                db_name: "default".to_string(),
                table_name: "broken_attached".to_string(),
            },
            table_meta: TableMeta {
                schema: broken_schema,
                engine: "FUSE".to_string(),
                options: [
                    (
                        OPT_KEY_DATABASE_ID.to_string(),
                        database.get_db_info().database_id.db_id.to_string(),
                    ),
                    (
                        FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE.to_string(),
                        "0".to_string(),
                    ),
                    (
                        OPT_KEY_TABLE_ATTACHED_DATA_URI.to_string(),
                        "s3://broken-bucket/broken-attached/".to_string(),
                    ),
                ]
                .into(),
                storage_params: Some(StorageParams::S3(StorageS3Config {
                    region: "us-east-2".to_string(),
                    endpoint_url: mock_server.uri(),
                    bucket: "broken-bucket".to_string(),
                    root: "/".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    disable_credential_loader: true,
                    ..Default::default()
                })),
                ..TableMeta::default()
            },
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        })
        .await?;

    let list_tables_error = catalog.list_tables(&tenant, "default").await;
    assert!(
        list_tables_error.is_err(),
        "loading attached tables without disabling refresh should fail"
    );

    let disabled_catalog = catalog.clone().disable_table_info_refresh()?;
    let disabled_list_tables = disabled_catalog.list_tables(&tenant, "default").await;
    assert!(
        disabled_list_tables.is_ok(),
        "loading attached tables with disable_table_info_refresh should succeed"
    );

    mock_server.reset().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let result = execute_query(ctx.clone(), "show tables from default").await?;
    let blocks = result.try_collect::<Vec<_>>().await?;
    let output = pretty_format_blocks(&blocks)?;
    println!("{}", output);

    assert!(
        output.contains("broken_attached"),
        "SHOW TABLES should keep the broken attached table visible: {output}"
    );
    assert!(
        output.contains("healthy"),
        "SHOW TABLES should still return healthy tables in the same database: {output}"
    );

    let warnings = ctx.pop_warnings();
    assert!(
        warnings.is_empty(),
        "SHOW TABLES should not emit storage warnings once refresh is disabled: {warnings:?}"
    );

    // The key regression check: listing system.tables must not touch table-level storage.
    let requests = mock_server
        .received_requests()
        .await
        .expect("request recording should be enabled");
    assert!(
        requests.is_empty(),
        "SHOW TABLES should not touch table-level storage when listing system.tables: {requests:?}"
    );

    Ok(())
}

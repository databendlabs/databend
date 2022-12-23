// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_catalog::table::Table;
use common_datablocks::pretty_format_blocks;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::AuthType;
use common_meta_types::RoleInfo;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserOption;
use common_meta_types::UserQuota;
use common_metrics::init_default_metrics_recorder;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storage::StorageParams;
use common_storage::StorageS3Config;
use common_storages_system::CatalogsTable;
use common_storages_system::ClustersTable;
use common_storages_system::ColumnsTable;
use common_storages_system::ConfigsTable;
use common_storages_system::ContributorsTable;
use common_storages_system::CreditsTable;
use common_storages_system::DatabasesTable;
use common_storages_system::EnginesTable;
use common_storages_system::FunctionsTable;
use common_storages_system::MetricsTable;
use common_storages_system::RolesTable;
use common_storages_system::SettingsTable;
use common_storages_system::TablesTableWithoutHistory;
use common_storages_system::TracingTable;
use common_storages_system::UsersTable;
use common_users::UserApiProvider;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::stream::ReadDataBlockStream;
use futures::TryStreamExt;
use goldenfile::Mint;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

async fn run_table_tests(
    file: &mut impl Write,
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
) -> Result<()> {
    let table_info = table.get_table_info();
    writeln!(file, "---------- TABLE INFO ------------").unwrap();
    writeln!(file, "{table_info}").unwrap();
    let source_plan = table.read_plan(ctx.clone(), None).await?;

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
                || item.contains("max_storage_io_requests"))
        });
    }
    for line in actual_lines {
        writeln!(file, "{}", line).unwrap();
    }
    write!(file, "\n\n").unwrap();
    Ok(())
}

#[tokio::test]
async fn test_columns_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("columns_table.txt").unwrap();
    let table = ColumnsTable::create(1);

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_clusters_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = ClustersTable::create(1);

    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    Ok(())
}

#[tokio::test]
async fn test_configs_table_basic() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("configs_table_basic.txt").unwrap();

    let conf = crate::tests::ConfigBuilder::create().config();
    let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None).await?;
    ctx.get_settings().set_max_threads(8)?;

    let table = ConfigsTable::create(1);

    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test]
async fn test_configs_table_redact() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let _file = &mut mint.new_goldenfile("configs_table_redact.txt").unwrap();

    let mock_server = MockServer::builder().start().await;
    Mock::given(method("HEAD"))
        .and(path("/test/.opendal"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = crate::tests::ConfigBuilder::create().build();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "test".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        ..Default::default()
    });

    let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None).await?;
    ctx.get_settings().set_max_threads(8)?;

    let table = ConfigsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    // need a method to skip/edit endpoint_url
    // run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test]
async fn test_contributors_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = ContributorsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);
    Ok(())
}

#[tokio::test]
async fn test_credits_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = CreditsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);
    Ok(())
}

#[tokio::test]
async fn test_catalogs_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("catalogs_table.txt").unwrap();

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = CatalogsTable::create(1);

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_databases_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = DatabasesTable::create(1);

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("databases_table.txt").unwrap();

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_engines_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = EnginesTable::create(1);

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("engines_table.txt").unwrap();

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_functions_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = FunctionsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 8);
    Ok(())
}

#[tokio::test]
async fn test_metrics_table() -> Result<()> {
    init_default_metrics_recorder();
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = MetricsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    metrics::counter!("test.test_metrics_table_count", 1);
    metrics::histogram!("test.test_metrics_table_histogram", 1.0);

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    assert!(block.num_rows() >= 1);

    let output = pretty_format_blocks(result.as_slice())?;
    assert!(output.contains("test_test_metrics_table_count"));
    assert!(output.contains("test_test_metrics_table_histogram"));

    Ok(())
}

#[tokio::test]
async fn test_roles_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("roles_table.txt").unwrap();

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    ctx.get_settings().set_max_threads(2)?;

    {
        let role_info = RoleInfo::new("test");
        UserApiProvider::instance()
            .add_role(&tenant, role_info, false)
            .await?;
    }

    {
        let mut role_info = RoleInfo::new("test1");
        role_info.grants.grant_role("test".to_string());
        UserApiProvider::instance()
            .add_role(&tenant, role_info, false)
            .await?;
    }
    let table = RolesTable::create(1);

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_settings_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("settings_table.txt").unwrap();

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_settings().set_max_memory_usage(1073741824)?;

    let table = SettingsTable::create(1);

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_tables_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table = TablesTableWithoutHistory::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 11);

    // check column "dropped_on"
    for x in &result {
        for row in 0..x.num_rows() {
            // index of column dropped_on is 6
            let column = x.column(6);
            let str = column.get_checked(row)?.to_string();
            // All of them should be NULL
            assert_eq!("NULL", str)
        }
    }

    // hard to tweak the regex assertion  just remove the column "dropped_on" :)
    let mut without_dropped = Vec::new();
    for x in result {
        without_dropped.push(x.remove_column("dropped_on")?)
    }

    let expected = vec![
        r"\+---------\+--------------------\+---------------------\+--------------------\+------------\+-------------------------------\+----------\+-----------\+----------------------\+------------\+",
        r"\| catalog \| database           \| name                \| engine             \| cluster_by \| created_on                    \| num_rows \| data_size \| data_compressed_size \| index_size \|",
        r"\+---------\+--------------------\+---------------------\+--------------------\+------------\+-------------------------------\+----------\+-----------\+----------------------\+------------\+",
        r"\| default \| information_schema \| columns             \| VIEW               \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| information_schema \| keywords            \| VIEW               \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| information_schema \| schemata            \| VIEW               \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| information_schema \| tables              \| VIEW               \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| information_schema \| views               \| VIEW               \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| catalogs            \| SystemCatalogs     \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| clustering_history  \| SystemLogTable     \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| clusters            \| SystemClusters     \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| columns             \| SystemColumns      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| configs             \| SystemConfigs      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| malloc_stats        \| SystemMetrics      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| malloc_stats_totals \| SystemMetrics      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| contributors        \| SystemContributors \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| credits             \| SystemCredits      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| databases           \| SystemDatabases    \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| engines             \| SystemEngines      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| functions           \| SystemFunctions    \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| metrics             \| SystemMetrics      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| one                 \| SystemOne          \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| processes           \| SystemProcesses    \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| query_log           \| SystemLogTable     \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| roles               \| SystemRoles        \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| settings            \| SystemSettings     \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| stages              \| SystemStages       \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| tables              \| SystemTables       \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| tables_with_history \| SystemTables       \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| tracing             \| SystemTracing      \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\| default \| system             \| users               \| SystemUsers        \|            \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+-]\d{4} \| NULL     \| NULL      \| NULL                 \| NULL       \|",
        r"\+---------\+--------------------\+---------------------\+--------------------\+------------\+-------------------------------\+----------\+-----------\+----------------------\+------------\+",
    ];
    common_datablocks::assert_blocks_sorted_eq_with_regex(expected, without_dropped.as_slice());
    // may need a method to work with regex
    // run_table_tests(file, ctx, table).await?;
    Ok(())
}

#[tokio::test]
async fn test_tracing_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let table: Arc<dyn Table> = Arc::new(TracingTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);
    assert!(block.num_rows() > 0);

    Ok(())
}

#[tokio::test]
async fn test_users_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("users_table.txt").unwrap();

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    ctx.get_settings().set_max_threads(2)?;
    let auth_data = AuthInfo::None;
    UserApiProvider::instance()
        .add_user(
            &tenant,
            UserInfo {
                auth_info: auth_data,
                name: "test".to_string(),
                hostname: "localhost".to_string(),
                grants: UserGrantSet::empty(),
                quota: UserQuota::no_limit(),
                option: UserOption::default(),
            },
            false,
        )
        .await?;
    let auth_data = AuthInfo::new(AuthType::Sha256Password, &Some("123456789".to_string()));
    assert!(auth_data.is_ok());
    UserApiProvider::instance()
        .add_user(
            &tenant,
            UserInfo {
                auth_info: auth_data.unwrap(),
                name: "test1".to_string(),
                hostname: "%".to_string(),
                grants: UserGrantSet::empty(),
                quota: UserQuota::no_limit(),
                option: UserOption::default().with_default_role(Some("role1".to_string())),
            },
            false,
        )
        .await?;

    let table = UsersTable::create(1);

    run_table_tests(file, ctx, table).await?;
    Ok(())
}

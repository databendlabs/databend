// Copyright 2022 Datafuse Labs.
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

use databend_common_ast::ast::Engine;
use databend_common_ast::parser::Dialect;
use databend_common_expression::ScalarRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::plans::AlterTableClusterKeyPlan;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::DropTableClusterKeyPlan;
use databend_common_storages_fuse::FuseTable;
use databend_query::interpreters::AlterTableClusterKeyInterpreter;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::DropTableClusterKeyInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::ShowCreateQuerySettings;
use databend_query::interpreters::ShowCreateTableInterpreter;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::HILBERT_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::LINEAR_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use futures_util::TryStreamExt;

fn show_create_settings() -> ShowCreateQuerySettings {
    ShowCreateQuerySettings {
        sql_dialect: Dialect::PostgreSQL,
        force_quoted_ident: false,
        unquoted_ident_case_sensitive: false,
        quoted_ident_case_sensitive: false,
        hide_options_in_show_create_table: true,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_hilbert_recluster_pipeline() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {}.{} (x INT, y INT) CLUSTER BY HILBERT(x, y) ROW_PER_BLOCK=2 BLOCK_PER_SEGMENT=2",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;
    for offset in [0, 10, 20, 30] {
        fixture
            .execute_command(&format!(
                "INSERT INTO {}.{} VALUES ({offset}, {}), ({}, {offset})",
                fixture.default_db_name(),
                fixture.default_table_name(),
                100 - offset,
                100 - offset,
            ))
            .await?;
    }
    fixture
        .execute_command(&format!(
            "ALTER TABLE {}.{} RECLUSTER FINAL",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;
    let blocks = fixture
        .execute_query(&format!(
            "SELECT count(), sum(x + y) FROM {}.{}",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let row = blocks[0].get_by_offset(0).index(0).unwrap();
    assert_eq!(row, ScalarRef::Number(8_u64.into()));
    let sum = blocks[0].get_by_offset(1).index(0).unwrap();
    assert_eq!(sum, ScalarRef::Number(800_i64.into()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_alter_table_cluster_key() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;

    let create_table_plan = CreateTablePlan {
        create_option: CreateOption::Create,
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        schema: TestFixture::default_table_schema(),
        engine: Engine::Fuse,
        engine_options: Default::default(),
        storage_params: None,
        options: [
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_comments: vec![],
        field_stats_truncate_len: vec![],
        as_select: None,
        cluster_key: None,
        table_indexes: None,
        table_constraints: None,
        attached_columns: None,
        table_partition: None,
        table_properties: None,
    };

    // create test table
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // add cluster key
    let alter_table_cluster_key_plan = AlterTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        branch: None,
        cluster_keys: vec!["id".to_string()],
        cluster_type: ClusterType::Linear,
    };
    let interpreter =
        AlterTableClusterKeyInterpreter::try_create(ctx.clone(), alter_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    assert_eq!(
        table_info.meta.cluster_key_v2,
        Some((1, "(id)".to_string()))
    );
    assert_eq!(table_info.meta.cluster_key_seq, 1);
    assert_eq!(
        table_info.meta.options.get(OPT_KEY_CLUSTER_TYPE),
        Some(&LINEAR_CLUSTER_TYPE.to_string())
    );
    assert_eq!(
        FuseTable::try_from_table(table.as_ref())?.cluster_type(),
        Some(ClusterType::Linear)
    );
    let create_sql =
        ShowCreateTableInterpreter::show_create_table_query(table_info, &show_create_settings())?;
    assert!(
        create_sql.contains("CLUSTER BY LINEAR(id)"),
        "unexpected SHOW CREATE output: {create_sql}"
    );

    // Switch the same table to Hilbert clustering.
    let alter_table_cluster_key_plan = AlterTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        branch: None,
        cluster_keys: vec!["id".to_string(), "id".to_string()],
        cluster_type: ClusterType::Hilbert,
    };
    let interpreter =
        AlterTableClusterKeyInterpreter::try_create(ctx.clone(), alter_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    assert_eq!(
        table_info.meta.cluster_key_v2,
        Some((2, "(id, id)".to_string()))
    );
    assert_eq!(table_info.meta.cluster_key_seq, 2);
    assert_eq!(
        table_info.meta.options.get(OPT_KEY_CLUSTER_TYPE),
        Some(&HILBERT_CLUSTER_TYPE.to_string())
    );
    assert_eq!(
        FuseTable::try_from_table(table.as_ref())?.cluster_type(),
        Some(ClusterType::Hilbert)
    );
    let create_sql =
        ShowCreateTableInterpreter::show_create_table_query(table_info, &show_create_settings())?;
    assert!(
        create_sql.contains("CLUSTER BY HILBERT(id, id)"),
        "unexpected SHOW CREATE output: {create_sql}"
    );

    // drop cluster key
    let drop_table_cluster_key_plan = DropTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        branch: None,
    };
    let interpreter =
        DropTableClusterKeyInterpreter::try_create(ctx.clone(), drop_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.cluster_key_v2, None);
    assert_eq!(table_info.meta.cluster_key_seq, 2);
    assert!(!table_info.meta.options.contains_key(OPT_KEY_CLUSTER_TYPE));

    Ok(())
}

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

use std::time::Duration;

use databend_common_ast::ast::Engine;
use databend_common_meta_api::SegmentClaimApi;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::DeleteSegmentClaimReq;
use databend_common_meta_app::schema::ListSegmentClaimsReq;
use databend_common_sql::plans::AlterTableClusterKeyPlan;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::DropTableClusterKeyPlan;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_storages_fuse::FuseTable;
use databend_common_users::UserApiProvider;
use databend_query::interpreters::AlterTableClusterKeyInterpreter;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::DropTableClusterKeyInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::locks::CoordinationManager;
use databend_query::sessions::TableContextQueryState;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::LINEAR_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_disjoint_recluster_commits() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let table = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    for values in ["(10), (30)", "(11), (31)", "(110), (130)", "(111), (131)"] {
        fixture
            .execute_command(&format!("INSERT INTO {table}(id) VALUES {values}"))
            .await?;
    }

    let table_before = fixture.latest_default_table().await?;
    let snapshot_before = FuseTable::try_from_table(table_before.as_ref())?
        .read_table_snapshot()
        .await?
        .expect("inserts should create a snapshot");
    assert_eq!(snapshot_before.segments.len(), 4);

    let low_ctx = fixture.new_query_ctx().await?;
    let high_ctx = fixture.new_query_ctx().await?;
    let low_query = format!("ALTER TABLE {table} RECLUSTER WHERE id < 100");
    let high_query = format!("ALTER TABLE {table} RECLUSTER WHERE id >= 100");
    tokio::try_join!(
        execute_command(low_ctx, &low_query),
        execute_command(high_ctx, &high_query)
    )?;

    let table_after = fixture.latest_default_table().await?;
    let snapshot_after = FuseTable::try_from_table(table_after.as_ref())?
        .read_table_snapshot()
        .await?
        .expect("recluster should leave a snapshot");
    assert_eq!(
        snapshot_after.summary.row_count,
        snapshot_before.summary.row_count
    );
    assert_eq!(snapshot_after.segments.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_claim_renew_failure_aborts_query() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("table_lock_expire_secs".to_string(), "3".to_string())?;
    let table_id = u64::MAX;
    let guard = CoordinationManager::instance()
        .try_segment_claim(ctx.clone(), table_id, vec!["segment".to_string()])
        .await?
        .expect("claim should succeed");

    let meta = UserApiProvider::instance().get_meta_store_client();
    let tenant = fixture.default_tenant();
    let claims = meta
        .list_segment_claims(ListSegmentClaimsReq {
            tenant: tenant.clone(),
            table_id,
        })
        .await?;
    let [(claim_id, _)] = claims.as_slice() else {
        panic!("expected exactly one segment claim");
    };
    let abort_notify = ctx.get_abort_notify();
    meta.delete_segment_claim(DeleteSegmentClaimReq {
        tenant,
        table_id,
        claim_id: *claim_id,
    })
    .await?;

    tokio::time::timeout(Duration::from_secs(5), abort_notify.notified())
        .await
        .expect("expired claim should abort its query");
    assert_eq!(
        ctx.get_error().expect("abort error should be set").code(),
        databend_common_exception::ErrorCode::LEASE_EXPIRED
    );
    drop(guard);
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
        target: MaintenanceTarget::Table,
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
    // drop cluster key
    let drop_table_cluster_key_plan = DropTableClusterKeyPlan {
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        target: MaintenanceTarget::Table,
        branch: None,
    };
    let interpreter =
        DropTableClusterKeyInterpreter::try_create(ctx.clone(), drop_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.cluster_key_v2, None);
    assert_eq!(table_info.meta.cluster_key_seq, 1);
    assert!(!table_info.meta.options.contains_key(OPT_KEY_CLUSTER_TYPE));

    Ok(())
}

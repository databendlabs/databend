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

use std::sync::Arc;

use chrono::Duration;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_query::interpreters::build_modify_column_physical_plan;
use databend_query::physical_plans::DistributedInsertSelect;
use databend_query::physical_plans::Exchange;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanCast;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

/// Build the rewrite plan of `ALTER TABLE ... MODIFY COLUMN b STRING` on a
/// `(a INT, b INT)` table.
async fn build_rewrite_plan(ctx: Arc<QueryContext>, db: &str, table: &str) -> Result<PhysicalPlan> {
    // The planner cache key does not include the cluster topology, so a plan
    // built in one context could be reused by the other. Plan each one fresh.
    ctx.get_settings()
        .set_setting("enable_planner_cache".to_string(), "0".to_string())?;

    let tbl = ctx.get_table(CATALOG_DEFAULT, db, table).await?;
    let mut table_info = tbl.get_table_info().clone();
    let mut new_schema = table_info.meta.schema.as_ref().clone();
    let (idx, _) = new_schema.column_with_name("b").expect("column b");
    new_schema.fields[idx].data_type = TableDataType::String;
    let new_schema = Arc::new(new_schema);
    table_info.meta.schema = new_schema.clone();

    let sql = format!("SELECT `a`, CAST(`b` AS STRING) AS `b` FROM `{db}`.`{table}`");
    let (plan, _) = build_modify_column_physical_plan(
        ctx,
        &sql,
        table_info,
        new_schema,
        TableMetaTimestamps::new(None, Duration::hours(1)),
    )
    .await?;
    Ok(plan)
}

/// The writer must carry the new schema, remote nodes build the table from it.
fn assert_writes_new_schema(insert: &DistributedInsertSelect) {
    let (_, b) = insert
        .table_info
        .meta
        .schema
        .column_with_name("b")
        .expect("column b");
    assert_eq!(b.data_type, TableDataType::String);
    assert!(insert.cast_needed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_modify_column_rewrite_physical_plan() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = "t_modify_column";
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.{table} (a INT NOT NULL, b INT NOT NULL) ENGINE=FUSE"
        ))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.{table} VALUES (1, 1), (2, 2)"))
        .await?;

    // Cluster: Merge(DistributedInsertSelect(...)), conversion and writes run on
    // every node, only writer metas are merged to the coordinator.
    let cluster_ctx = fixture
        .new_query_ctx_with_cluster(
            ClusterDescriptor::new()
                .with_node_info("node-a", "127.0.0.1:9090", "cluster", "warehouse")
                .with_node_info("node-b", "127.0.0.1:9091", "cluster", "warehouse")
                .with_local_id("node-a"),
        )
        .await?;
    let plan = build_rewrite_plan(cluster_ctx, &db, table).await?;
    let merge = Exchange::from_physical_plan(&plan)
        .unwrap_or_else(|| panic!("expect top Merge exchange:\n{plan:?}"));
    assert_eq!(merge.kind, FragmentKind::Merge);
    let insert = DistributedInsertSelect::from_physical_plan(&merge.input)
        .unwrap_or_else(|| panic!("expect DistributedInsertSelect below Merge:\n{plan:?}"));
    assert_writes_new_schema(insert);

    // Single node: no exchange.
    let local_ctx = fixture.new_query_ctx().await?;
    let plan = build_rewrite_plan(local_ctx, &db, table).await?;
    assert!(
        Exchange::from_physical_plan(&plan).is_none(),
        "expect no exchange on a single node:\n{plan:?}"
    );
    let insert = DistributedInsertSelect::from_physical_plan(&plan)
        .unwrap_or_else(|| panic!("expect top DistributedInsertSelect:\n{plan:?}"));
    assert_writes_new_schema(insert);

    Ok(())
}

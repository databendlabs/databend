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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_query;
use futures_util::TryStreamExt;

async fn run_query_pretty(ctx: Arc<QueryContext>, sql: &str) -> Result<String> {
    let blocks = execute_query(ctx, sql)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    if blocks.is_empty() {
        return Ok("<empty>".to_string());
    }
    Ok(pretty_format_blocks(&blocks)?.to_string())
}

async fn prepare_spatial_join_tables(fixture: &TestFixture, db: &str) -> Result<()> {
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.spatial_join_left(\
             id INT, \
             geom GEOMETRY, \
             SPATIAL INDEX idx (geom)) \
             ENGINE=Fuse row_per_block=1"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.spatial_join_right(\
             id INT, \
             geom GEOMETRY, \
             SPATIAL INDEX idx (geom)) \
             ENGINE=Fuse row_per_block=1"
        ))
        .await?;

    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.spatial_join_left VALUES \
             (1, TO_GEOMETRY('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')), \
             (2, TO_GEOMETRY('POLYGON((10 0, 10 5, 15 5, 15 0, 10 0))')), \
             (3, TO_GEOMETRY('POLYGON((20 0, 20 5, 25 5, 25 0, 20 0))'))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.spatial_join_right VALUES \
             (10, TO_GEOMETRY('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))')), \
             (20, TO_GEOMETRY('POLYGON((11 1, 11 2, 12 2, 12 1, 11 1))')), \
             (30, TO_GEOMETRY('POLYGON((100 100, 100 101, 101 101, 101 100, 100 100))'))"
        ))
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_join_matches_main_path_and_explain() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    let db = fixture.default_db_name();
    prepare_spatial_join_tables(&fixture, &db).await?;

    let join_sql = format!(
        "SELECT l.id, r.id \
         FROM {db}.spatial_join_left AS l \
         JOIN {db}.spatial_join_right AS r \
         ON ST_Intersects(l.geom, r.geom) \
         ORDER BY l.id, r.id"
    );

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "0".to_string())?;
    let main_result = run_query_pretty(ctx.clone(), &join_sql).await?;

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let spatial_result = run_query_pretty(ctx.clone(), &join_sql).await?;
    assert_eq!(spatial_result, main_result);

    let explain = run_query_pretty(ctx.clone(), &format!("EXPLAIN {join_sql}")).await?;
    assert!(
        explain.contains("SpatialJoin"),
        "expected EXPLAIN to contain SpatialJoin, got:\n{explain}"
    );

    let cluster_ctx = fixture
        .new_query_ctx_with_cluster(
            ClusterDescriptor::new()
                .with_node_info("node-a", "127.0.0.1:9090", "node-a", "node-a")
                .with_node_info("node-b", "127.0.0.1:9091", "node-b", "node-b")
                .with_local_id("node-a"),
        )
        .await?;
    cluster_ctx
        .get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let cluster_explain = run_query_pretty(cluster_ctx, &format!("EXPLAIN {join_sql}")).await?;
    assert!(
        cluster_explain.contains("BroadcastSpatialJoin"),
        "expected cluster plan to use BroadcastSpatialJoin, got:\n{cluster_explain}"
    );
    assert!(
        cluster_explain.contains("exchange type: Broadcast"),
        "expected BroadcastSpatialJoin build child to contain broadcast exchange, got:\n{cluster_explain}"
    );

    let cluster_ctx = fixture
        .new_query_ctx_with_cluster(
            ClusterDescriptor::new()
                .with_node_info("node-a", "127.0.0.1:9090", "node-a", "node-a")
                .with_node_info("node-b", "127.0.0.1:9091", "node-b", "node-b")
                .with_local_id("node-a"),
        )
        .await?;
    cluster_ctx
        .get_settings()
        .set_setting("enable_join_runtime_filter".to_string(), "0".to_string())?;
    cluster_ctx
        .get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let topn_join_sql = format!(
        "SELECT l.id, r.id \
         FROM {db}.spatial_join_left AS l \
         JOIN (\
             SELECT id, geom \
             FROM {db}.spatial_join_right \
             ORDER BY id \
             LIMIT 2\
         ) AS r \
         ON ST_Intersects(l.geom, r.geom) \
         ORDER BY l.id, r.id"
    );
    let topn_cluster_explain =
        run_query_pretty(cluster_ctx, &format!("EXPLAIN {topn_join_sql}")).await?;
    assert!(
        topn_cluster_explain.contains("SpatialJoin"),
        "expected top-N build side cluster plan to use SpatialJoin, got:\n{topn_cluster_explain}"
    );
    assert!(
        !topn_cluster_explain.contains("BroadcastSpatialJoin"),
        "expected top-N build side cluster plan to avoid BroadcastSpatialJoin, got:\n{topn_cluster_explain}"
    );

    ctx.get_settings()
        .set_setting("spatial_join_max_build_rows".to_string(), "0".to_string())?;
    let fallback_explain = run_query_pretty(ctx, &format!("EXPLAIN {join_sql}")).await?;
    assert!(
        !fallback_explain.contains("SpatialJoin"),
        "expected max_build_rows=0 to skip SpatialJoin, got:\n{fallback_explain}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_join_dwithin_expands_bbox_candidates() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.spatial_join_dwithin_left(id INT, geom GEOMETRY)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.spatial_join_dwithin_right(id INT, geom GEOMETRY)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.spatial_join_dwithin_left VALUES \
             (1, TO_GEOMETRY('POINT(0 0)'))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {db}.spatial_join_dwithin_right VALUES \
             (10, TO_GEOMETRY('POINT(1.5 0)')), \
             (20, TO_GEOMETRY('POINT(5 0)'))"
        ))
        .await?;

    let join_sql = format!(
        "SELECT l.id, r.id \
         FROM {db}.spatial_join_dwithin_left AS l \
         JOIN {db}.spatial_join_dwithin_right AS r \
         ON ST_DWithin(l.geom, r.geom, 2.0) \
         ORDER BY l.id, r.id"
    );

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "0".to_string())?;
    let main_result = run_query_pretty(ctx.clone(), &join_sql).await?;

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let spatial_result = run_query_pretty(ctx.clone(), &join_sql).await?;
    assert_eq!(spatial_result, main_result);

    let explain = run_query_pretty(ctx, &format!("EXPLAIN {join_sql}")).await?;
    assert!(
        explain.contains("SpatialJoin"),
        "expected DWithin EXPLAIN to contain SpatialJoin, got:\n{explain}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_join_preserves_build_side_cache_join() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let db = fixture.default_db_name();
    prepare_spatial_join_tables(&fixture, &db).await?;

    let cache_join_sql = format!(
        "SELECT l.id \
         FROM {db}.spatial_join_left AS l \
         JOIN LATERAL (VALUES (l.geom)) AS v (geom) \
         ON ST_Intersects(l.geom, v.geom) \
         ORDER BY l.id"
    );

    let explain = run_query_pretty(ctx.clone(), &format!("EXPLAIN {cache_join_sql}")).await?;
    assert!(
        !explain.contains("SpatialJoin"),
        "build-side cache join must stay on the hash path, got:\n{explain}"
    );

    run_query_pretty(ctx, &cache_join_sql).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spatial_join_falls_back_with_residual_predicate() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;
    let db = fixture.default_db_name();
    prepare_spatial_join_tables(&fixture, &db).await?;

    let residual_join_sql = format!(
        "SELECT l.id, r.id \
         FROM {db}.spatial_join_left AS l \
         JOIN {db}.spatial_join_right AS r \
         ON l.id < r.id AND ST_Intersects(l.geom, r.geom) \
         ORDER BY l.id, r.id"
    );

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "0".to_string())?;
    let main_result = run_query_pretty(ctx.clone(), &residual_join_sql).await?;

    ctx.get_settings()
        .set_setting("enable_spatial_join".to_string(), "1".to_string())?;
    let spatial_result = run_query_pretty(ctx.clone(), &residual_join_sql).await?;
    assert_eq!(spatial_result, main_result);

    let explain = run_query_pretty(ctx, &format!("EXPLAIN {residual_join_sql}")).await?;
    assert!(
        !explain.contains("SpatialJoin"),
        "residual-predicate join must fall back to the main path, got:\n{explain}"
    );

    Ok(())
}

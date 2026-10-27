// Copyright 2026 Datafuse Labs
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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

/// Start from SQL and the production optimizer/physical builder, rather than
/// constructing a scan that the optimizer might never actually produce.
#[tokio::test(flavor = "multi_thread")]
async fn test_fts_top_k_pushdown_from_sql() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let table = format!("{}.fts_topk", fixture.default_db_name());
    fixture
        .execute_command(&format!(
            "CREATE TABLE {table} (id INT, body STRING, \
             INVERTED INDEX fts (body) tokenizer = 'english') \
             ROW_PER_BLOCK = 64 STORAGE_FORMAT = 'parquet'"
        ))
        .await?;
    // Separate inserts ensure multiple blocks, each with more matches than K.
    for block in 0..2 {
        fixture
            .execute_command(&format!(
                "INSERT INTO {table} SELECT number + {block} * 16, \
                 concat(repeat('alpha ', number + 1), repeat('beta ', 16 - number)) \
                 FROM numbers(16)"
            ))
            .await?;
    }

    let cases = [
        ("ORDER BY score() DESC LIMIT 3", Some(3)),
        ("ORDER BY score() DESC LIMIT 3 OFFSET 5", Some(8)),
        ("ORDER BY score() DESC NULLS FIRST LIMIT 3", Some(3)),
        ("ORDER BY score() DESC LIMIT 100", Some(100)),
        ("ORDER BY score() ASC LIMIT 3", None),
        ("ORDER BY score() DESC, id DESC LIMIT 3", None),
        ("ORDER BY -score() ASC LIMIT 3", None),
        ("ORDER BY score() DESC", None),
        ("LIMIT 3", None),
        ("AND id < 2 ORDER BY score() DESC LIMIT 3", None),
        ("AND score() < 1 ORDER BY score() DESC LIMIT 3", None),
    ];
    // Exercise eager partition pruning and the deferred pruning pipeline. The
    // exact same eligibility must reach storage in both execution modes.
    for pruning_pipeline in [0, 1] {
        for (suffix, expected) in cases {
            let sql =
                format!("SELECT id, score() FROM {table} WHERE match(body, 'alpha') {suffix}");
            let ctx = fixture.new_query_ctx().await?;
            ctx.get_settings().set_setting(
                "enable_prune_pipeline".to_string(),
                pruning_pipeline.to_string(),
            )?;
            ctx.get_settings().set_setting(
                "enable_distributed_pruning".to_string(),
                pruning_pipeline.to_string(),
            )?;
            let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
            let Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } = plan
            else {
                panic!("expected query plan: {sql}");
            };
            let physical = PhysicalPlanBuilder::new(metadata, ctx, false)
                .build_query(s_expr.planned()?, bind_context.column_set())
                .await?;
            let mut sources = Vec::new();
            physical.get_all_data_source(&mut sources);
            assert_eq!(sources.len(), 1, "{sql}");
            let source = &sources[0].1;
            assert_eq!(
                source
                    .push_downs
                    .as_ref()
                    .and_then(PushDownInfo::inverted_index_top_k),
                expected,
                "pipeline={pruning_pipeline}: {sql}"
            );
            if pruning_pipeline == 0
                && let Some(k) = expected
            {
                assert!(!source.parts.partitions.is_empty(), "{sql}");
                for part in &source.parts.partitions {
                    let part = FuseBlockPartInfo::from_part(part)?;
                    let index = part.block_meta_index.as_ref().unwrap();
                    let rows = index.matched_rows.as_ref().unwrap();
                    let scores = index.matched_scores.as_ref().unwrap();
                    assert_eq!(rows.len(), k.min(16), "{sql}");
                    assert_eq!(scores.len(), rows.len(), "{sql}");
                }
            }
        }
    }
    Ok(())
}

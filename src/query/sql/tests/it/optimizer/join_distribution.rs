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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use crate::framework::LiteTableContext;

#[derive(Default)]
struct Exchanges {
    node_to_node_key_counts: Vec<usize>,
    global_hash: usize,
    broadcast: usize,
}

fn collect_exchanges(expr: &SExpr, exchanges: &mut Exchanges) {
    if let RelOperator::Exchange(exchange) = expr.plan() {
        match exchange {
            Exchange::NodeToNodeHash(keys) => {
                exchanges.node_to_node_key_counts.push(keys.len());
            }
            Exchange::GlobalHash(_) => exchanges.global_hash += 1,
            Exchange::Broadcast => exchanges.broadcast += 1,
            _ => {}
        }
    }

    for child in expr.children() {
        collect_exchanges(child, exchanges);
    }
}

async fn optimized_join(ctx: &std::sync::Arc<LiteTableContext>) -> Result<SExpr> {
    let raw = ctx
        .bind_sql(
            "SELECT l.payload, r.payload \
             FROM join_distribution_l AS l \
             INNER JOIN join_distribution_r AS r \
               ON l.k1 = r.k1 AND l.k2 = r.k2",
        )
        .await?;
    let Plan::Query { s_expr, .. } = ctx.optimize_plan(raw).await? else {
        unreachable!("join query should produce a query plan");
    };
    Ok(*s_expr)
}

async fn optimized_nullable_mark_join(ctx: &std::sync::Arc<LiteTableContext>) -> Result<SExpr> {
    let raw = ctx
        .bind_sql(
            "SELECT l.payload IN (SELECT r.payload FROM join_distribution_r AS r) \
             FROM join_distribution_l AS l",
        )
        .await?;
    let Plan::Query { s_expr, .. } = ctx.optimize_plan(raw).await? else {
        unreachable!("mark join query should produce a query plan");
    };
    Ok(*s_expr)
}

async fn optimized_correlated_nullable_mark_join(
    ctx: &std::sync::Arc<LiteTableContext>,
) -> Result<SExpr> {
    let raw = ctx
        .bind_sql(
            "SELECT l.k1, l.k2, l.k1 IN (\
                 SELECT r.k1 FROM join_distribution_r AS r WHERE l.k2 = r.k2\
             ) \
             FROM join_distribution_l AS l",
        )
        .await?;
    let Plan::Query { s_expr, .. } = ctx.optimize_plan(raw).await? else {
        unreachable!("correlated mark join query should produce a query plan");
    };
    Ok(*s_expr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_hash_join_child_distributions() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(false)?;
    ctx.set_cluster_node_num(3);
    ctx.set_table_warehouse_distribution(true);
    ctx.register_table_sql(
        "CREATE TABLE join_distribution_l (k1 INT NOT NULL, k2 STRING NOT NULL, payload INT)",
    )
    .await?;
    ctx.register_table_sql(
        "CREATE TABLE join_distribution_r (k1 INT, k2 STRING NOT NULL, payload INT)",
    )
    .await?;

    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "1".to_string())?;
    let shuffle_plan = optimized_join(&ctx).await?;
    let mut shuffle_exchanges = Exchanges::default();
    collect_exchanges(&shuffle_plan, &mut shuffle_exchanges);
    shuffle_exchanges.node_to_node_key_counts.sort_unstable();
    assert_eq!(shuffle_exchanges.node_to_node_key_counts, vec![2, 2]);
    assert_eq!(shuffle_exchanges.global_hash, 0);
    assert_eq!(shuffle_exchanges.broadcast, 0);

    let mark_plan = optimized_nullable_mark_join(&ctx).await?;
    let mut mark_exchanges = Exchanges::default();
    collect_exchanges(&mark_plan, &mut mark_exchanges);
    assert_eq!(mark_exchanges.node_to_node_key_counts, vec![1]);
    assert_eq!(mark_exchanges.global_hash, 0);
    assert_eq!(mark_exchanges.broadcast, 1);

    let correlated_mark_plan = optimized_correlated_nullable_mark_join(&ctx).await?;
    let mut correlated_mark_exchanges = Exchanges::default();
    collect_exchanges(&correlated_mark_plan, &mut correlated_mark_exchanges);
    // Correlated Mark Join keeps nullable marker state in one join instance,
    // so both inputs are merged instead of hash-shuffled or broadcast.
    assert!(correlated_mark_exchanges.node_to_node_key_counts.is_empty());
    assert_eq!(correlated_mark_exchanges.global_hash, 0);
    assert_eq!(correlated_mark_exchanges.broadcast, 0);

    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "0".to_string())?;
    ctx.get_settings()
        .set_setting("enforce_broadcast_join".to_string(), "1".to_string())?;
    let broadcast_plan = optimized_join(&ctx).await?;
    let mut broadcast_exchanges = Exchanges::default();
    collect_exchanges(&broadcast_plan, &mut broadcast_exchanges);
    assert_eq!(broadcast_exchanges.broadcast, 1);
    assert!(broadcast_exchanges.node_to_node_key_counts.is_empty());

    Ok(())
}

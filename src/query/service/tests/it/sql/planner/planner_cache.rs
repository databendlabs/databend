// Copyright 2026 Datafuse Labs.
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

use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::TestFixture;

/// Plans `sql` on a cluster of `nodes`, the first of them local, and tells whether the plan is
/// distributed.
async fn plans_distributed(fixture: &TestFixture, nodes: &[&str], sql: &str) -> Result<bool> {
    let mut cluster = ClusterDescriptor::new();
    for (i, node) in nodes.iter().enumerate() {
        cluster = cluster.with_node_info(
            *node,
            format!("127.0.0.1:{}", 9090 + i),
            "cluster",
            "warehouse",
        );
    }
    let ctx = fixture
        .new_query_ctx_with_cluster(cluster.with_local_id(nodes[0]))
        .await?;
    let (plan, _) = Planner::new(ctx).plan_sql(sql).await?;
    let Plan::Query { s_expr, .. } = plan else {
        panic!("expected a query plan for {sql}");
    };
    Ok(s_expr.planned()?.expr().has_merge_exchange())
}

// A plan cached on a cluster is not served to a single node, where it would fail, and a plan
// cached on a single node is not served to a cluster, which it would leave unused.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_planner_cache_keeps_distributed_and_local_plans_apart() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!("CREATE TABLE {db}.t (n INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.t VALUES (1)"))
        .await?;

    let cluster = ["node-a", "node-b"];
    let single_node = ["node-a"];

    let sql = format!("SELECT count(*) FROM {db}.t WHERE n > 0");
    assert!(plans_distributed(&fixture, &cluster, &sql).await?);
    assert!(!plans_distributed(&fixture, &single_node, &sql).await?);

    let sql = format!("SELECT count(*) FROM {db}.t WHERE n > 1");
    assert!(!plans_distributed(&fixture, &single_node, &sql).await?);
    assert!(plans_distributed(&fixture, &cluster, &sql).await?);
    Ok(())
}

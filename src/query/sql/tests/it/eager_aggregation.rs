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

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::Matcher;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::optimizer::optimizers::operator::PullUpFilterOptimizer;
use databend_common_sql::optimizer::optimizers::operator::RuleNormalizeAggregateOptimizer;
use databend_common_sql::optimizer::optimizers::operator::RuleStatsAggregateOptimizer;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use databend_common_sql::optimizer::optimizers::rule::Rule;
use databend_common_sql::optimizer::optimizers::rule::RuleEagerAggregation;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::optimizer::optimizers::rule::TransformResult;
use databend_common_sql::plans::Plan;
use goldenfile::Mint;

use crate::planner::LiteTableContext;

#[tokio::test(flavor = "multi_thread")]
async fn test_eager_aggregation_with_lite_table_context() -> Result<()> {
    let mut mint = Mint::new("tests/it");
    let mut file = mint.new_goldenfile("eager_aggregation.txt")?;

    let ctx = LiteTableContext::create().await?;
    for sql in [CUSTOMER_TABLE, ORDERS_TABLE, LINEITEM_TABLE] {
        ctx.register_table_sql(sql).await?;
    }

    for (idx, sql) in [Q0, Q1, Q2, Q3, Q4, Q5].iter().copied().enumerate() {
        run_eager_aggregation_query(&mut file, &ctx, idx, sql).await?;
    }

    Ok(())
}

async fn run_eager_aggregation_query(
    file: &mut File,
    ctx: &Arc<LiteTableContext>,
    idx: usize,
    sql: &str,
) -> Result<()> {
    writeln!(file, "=== #{idx} sql ===")?;
    writeln!(file, "{sql}\n")?;

    let plan = ctx.bind_sql(sql).await?;

    let Plan::Query {
        s_expr, metadata, ..
    } = &plan
    else {
        unreachable!()
    };

    let settings = ctx.get_settings();
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone()).with_settings(&settings)?;

    let before_expr = optimize_before(opt_ctx, s_expr).await?;
    let before_plan = plan.replace_query_s_expr(before_expr.clone());

    writeln!(file, "=== #{idx} raw plan ===")?;
    writeln!(file, "{}", before_plan.format_indent(Default::default())?)?;

    let mut state = TransformResult::new();
    let rule = RuleEagerAggregation::new(metadata.clone());

    for (matcher_idx, matcher) in rule.matchers().iter().enumerate() {
        let mut extractor = Extractor {
            matcher,
            result: None,
        };
        before_expr.accept(&mut extractor)?;
        if let Some(s_expr) = extractor.result {
            rule.apply_matcher(matcher_idx, &s_expr, &mut state)?;
            if !state.results().is_empty() {
                break;
            }
        }
    }

    for (result_idx, result) in state.results().iter().enumerate() {
        writeln!(file, "=== #{idx} apply plan {result_idx} ===")?;
        let rewritten = plan.replace_query_s_expr(result.clone());
        writeln!(file, "{}", rewritten.format_indent(Default::default())?)?;
    }

    Ok(())
}

struct Extractor<'a> {
    matcher: &'a Matcher,
    result: Option<SExpr>,
}

impl SExprVisitor for Extractor<'_> {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if self.matcher.matches(expr) {
            self.result = Some(expr.clone());
            Ok(VisitAction::Stop)
        } else {
            Ok(VisitAction::Continue)
        }
    }
}

async fn optimize_before(opt_ctx: Arc<OptimizerContext>, s_expr: &SExpr) -> Result<SExpr> {
    let s_expr = RuleStatsAggregateOptimizer::new(opt_ctx.clone())
        .optimize_async(s_expr)
        .await?;
    let s_expr = RuleNormalizeAggregateOptimizer::new().optimize_sync(&s_expr)?;
    let s_expr = PullUpFilterOptimizer::new(opt_ctx.clone()).optimize_sync(&s_expr)?;
    let s_expr = RecursiveRuleOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES)
        .optimize_sync(&s_expr)?;
    RecursiveRuleOptimizer::new(opt_ctx, &[RuleID::SplitAggregate]).optimize_sync(&s_expr)
}

const Q0: &str = "SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    orders join customer on c_custkey = o_custkey,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate";

const Q1: &str = "SELECT o_orderkey, sum(l_extendedprice * (1-l_discount))
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
AND l_returnflag = 'R'
GROUP BY o_orderkey";

const Q2: &str = "SELECT o_orderkey, sum(l_extendedprice), sum(o_totalprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey";

const Q3: &str = "SELECT o_orderkey, sum(revenue)
    FROM (SELECT l_orderkey, sum(l_extendedprice * (1-l_discount)) as revenue
        FROM lineitem WHERE l_returnflag = 'R' GROUP BY l_orderkey) as loss, orders
    WHERE o_orderkey = l_orderkey
    AND o_orderdate BETWEEN CAST('1995-05-01' as date) AND CAST('1995-05-31' as date)
    GROUP BY o_orderkey";

const Q4: &str = "SELECT o_orderkey, count(*)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey";

const Q5: &str = "SELECT o_orderkey, sum(l_extendedprice) + 1
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey";

const CUSTOMER_TABLE: &str = "CREATE TABLE customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DECIMAL(15, 2)   not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
)";

const ORDERS_TABLE: &str = "CREATE TABLE orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    STRING not null,
    o_totalprice     DECIMAL(15, 2) not null,
    o_orderdate      DATE not null,
    o_orderpriority  STRING not null,
    o_clerk          STRING not null,
    o_shippriority   INTEGER not null,
    o_comment        STRING not null
)";

const LINEITEM_TABLE: &str = "CREATE TABLE lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DECIMAL(15, 2) not null,
    l_extendedprice  DECIMAL(15, 2) not null,
    l_discount    DECIMAL(15, 2) not null,
    l_tax         DECIMAL(15, 2) not null,
    l_returnflag  STRING not null,
    l_linestatus  STRING not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode     STRING not null,
    l_comment      STRING not null
)";

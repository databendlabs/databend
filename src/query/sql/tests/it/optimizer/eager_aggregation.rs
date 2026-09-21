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
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::Rule;
use databend_common_sql::optimizer::optimizers::rule::RuleEagerAggregation;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::optimizer::optimizers::rule::TransformResult;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eager_aggregation_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "eager_aggregation.txt")?;

    let cases = [
        SqlTestCase {
            name: "count_star_can_preaggregate_build_side",
            description: "COUNT(*) grouped by the join key should allow eager aggregation on one side.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, count(*)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "sum_plus_constant_preserves_eager_aggregation",
            description: "A SUM output used inside a scalar expression should still optimize through eager aggregation.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, sum(l_extendedprice) + 1
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "count_plus_constant_preserves_eager_aggregation",
            description: "A COUNT output used inside a scalar expression should still optimize through eager aggregation.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, count(*) + 1
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "sum_distinct_is_not_eager",
            description: "Distinct sums cannot combine finalized local sums.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, sum_distinct(l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "count_distinct_is_not_eager",
            description: "Distinct counts cannot combine finalized local counts.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, count_distinct(l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "semantic_distinct_is_not_eager",
            description: "Semantic DISTINCT must resolve to an unsupported eager strategy.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, sum(DISTINCT l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "stddev_is_not_eager",
            description: "Mergeable variance state does not make finalized standard deviations composable.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, stddev_pop(l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "uniq_is_not_eager",
            description: "Distinct counts with a dedicated name must not be eager.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, uniq(l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "min_max_can_preaggregate",
            description: "Extrema combine finalized local extrema without multiplicity compensation.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, min(l_extendedprice), max(l_extendedprice)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eager_aggregation_keeps_decimal_product_types_in_sync() -> Result<()> {
    let case = SqlTestCase {
        name: "decimal_sum_multiplied_by_eager_count",
        description: "",
        setup_sqls: &[DECIMAL_SALES_TABLE, DATE_DIM_TABLE],
        sql: "SELECT ss_store_sk, sum(ss_ext_sales_price)
FROM store_sales CROSS JOIN date_dim
GROUP BY ss_store_sk",
    };
    let ctx = setup_context(&case).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(case.sql).await?
    else {
        unreachable!("test query should bind to Plan::Query")
    };

    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?);
    let split =
        RecursiveRuleOptimizer::new(opt_ctx, &[RuleID::SplitAggregate]).optimize_sync(*s_expr)?;
    let rewritten = RuleEagerAggregation::new(metadata.clone()).optimize_sync(&split)?;
    rewritten.validate_types(&metadata)?;

    Ok(())
}

// Exercise candidate generation directly as well as the production-plan goldens:
// the cost model may discard a legal candidate, hiding an eligibility regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eager_aggregation_strategy_candidates() -> Result<()> {
    for (aggregate, eligible) in [
        ("sum", true),
        ("count", true),
        ("min", true),
        ("max", true),
        ("min_distinct", true),
        ("sum_distinct", false),
        ("count_distinct", false),
        ("uniq", false),
        ("stddev_pop", false),
    ] {
        let sql = format!(
            "SELECT ss_store_sk, {aggregate}(ss_ext_sales_price) + 1
FROM store_sales CROSS JOIN date_dim
GROUP BY ss_store_sk"
        );
        let ctx = LiteTableContext::create().await?;
        ctx.register_setup_sql(DECIMAL_SALES_TABLE).await?;
        ctx.register_setup_sql(DATE_DIM_TABLE).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = ctx.bind_sql(&sql).await?
        else {
            unreachable!("test query should bind to Plan::Query")
        };
        let opt_ctx =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?);
        let split = RecursiveRuleOptimizer::new(opt_ctx, &[RuleID::SplitAggregate])
            .optimize_sync(*s_expr)?;
        let mut results = TransformResult::new();
        RuleEagerAggregation::new(metadata.clone()).apply(&split, &mut results)?;
        assert_eq!(!results.results().is_empty(), eligible, "{aggregate}");
        for result in results.results() {
            result.validate_types(&metadata)?;
        }
    }
    Ok(())
}

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
    l_linenumber  INTEGER not null,
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

const DECIMAL_SALES_TABLE: &str = "CREATE TABLE store_sales
(
    ss_store_sk          INTEGER,
    ss_ext_sales_price   DECIMAL(7, 2)
)";

const DATE_DIM_TABLE: &str = "CREATE TABLE date_dim
(
    d_date_sk INTEGER
)";

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

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(file, "{}", optimized_plan.format_indent(Default::default())?)?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eager_aggregation_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "eager_aggregation.txt")?;

    let cases = [
        SqlTestCase {
            name: "count_star_can_preaggregate_build_side",
            description:
                "COUNT(*) grouped by the join key should allow eager aggregation on one side.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, count(*)
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "sum_plus_constant_preserves_eager_aggregation",
            description:
                "A SUM output used inside a scalar expression should still optimize through eager aggregation.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, sum(l_extendedprice) + 1
FROM lineitem, orders
WHERE o_orderkey = l_orderkey
GROUP BY o_orderkey",
        },
        SqlTestCase {
            name: "count_plus_constant_preserves_eager_aggregation",
            description:
                "A COUNT output used inside a scalar expression should still optimize through eager aggregation.",
            setup_sqls: &[ORDERS_TABLE, LINEITEM_TABLE],
            sql: "SELECT o_orderkey, count(*) + 1
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

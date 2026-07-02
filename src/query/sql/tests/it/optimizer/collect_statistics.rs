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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::TableStatistics;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Scan;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::ColumnTopNEntry;
use databend_storages_common_table_meta::table::ChangeType;

use crate::framework::FrequencyStatsMap;
use crate::framework::LiteTableContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_collect_statistics_skips_top_n_for_change_scan() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;

    let top_n = ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(1)),
            count: 90,
            error: 0,
        }],
        min_index: None,
    };
    let mut count_min_sketch = ColumnCountMinSketch::default();
    count_min_sketch.add_with_count(Scalar::Number(NumberScalar::UInt64(1)).as_ref(), 90);
    ctx.register_table_with_stats_and_frequency_stats(
        "default",
        "t",
        vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::UInt64),
        )],
        Some(TableStatistics {
            num_rows: Some(100),
            data_size: Some(800),
            number_of_blocks: Some(1),
            number_of_segments: Some(1),
            ..Default::default()
        }),
        HashMap::new(),
        HashMap::new(),
        FrequencyStatsMap {
            top_n: HashMap::from([("a".to_string(), top_n)]),
            count_min_sketch: HashMap::from([("a".to_string(), count_min_sketch)]),
        },
        BTreeMap::new(),
    )?;

    let normal_plan = ctx
        .optimize_plan(ctx.bind_sql("select a from t").await?)
        .await?;
    let normal_scan = find_scan(&normal_plan);
    assert!(!normal_scan.statistics.top_n.is_empty());
    assert!(!normal_scan.statistics.count_min_sketch.is_empty());

    let change_plan = set_scan_change_type(
        ctx.bind_sql("select a from t").await?,
        Some(ChangeType::Append),
    );
    let change_plan = ctx.optimize_plan(change_plan).await?;
    let change_scan = find_scan(&change_plan);
    assert!(change_scan.statistics.top_n.is_empty());
    assert!(change_scan.statistics.count_min_sketch.is_empty());

    Ok(())
}

fn set_scan_change_type(plan: Plan, change_type: Option<ChangeType>) -> Plan {
    match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } => Plan::Query {
            s_expr: Box::new(set_s_expr_scan_change_type(&s_expr, change_type)),
            metadata,
            bind_context,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        },
        _ => unreachable!("test query should bind to Plan::Query"),
    }
}

fn set_s_expr_scan_change_type(s_expr: &SExpr, change_type: Option<ChangeType>) -> SExpr {
    let plan = match s_expr.plan() {
        RelOperator::Scan(scan) => {
            let mut scan = scan.clone();
            scan.change_type = change_type.clone();
            Arc::new(RelOperator::Scan(scan))
        }
        _ => s_expr.plan.clone(),
    };
    let children = s_expr
        .children()
        .map(|child| Arc::new(set_s_expr_scan_change_type(child, change_type.clone())))
        .collect();
    SExpr::create(plan, children, None, None, None)
}

fn find_scan(plan: &Plan) -> &Scan {
    match plan {
        Plan::Query { s_expr, .. } => find_scan_in_s_expr(s_expr).unwrap(),
        _ => unreachable!("test optimizer should return Plan::Query"),
    }
}

fn find_scan_in_s_expr(s_expr: &SExpr) -> Option<&Scan> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Some(scan),
        _ => s_expr.children().find_map(find_scan_in_s_expr),
    }
}

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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Sample;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::SampleLevel;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use num_traits::ToPrimitive;

use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::statistics::CollectStatisticsOptimizer;
use crate::optimizer::QuerySampleExecutor;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::SelectivityEstimator;
use crate::optimizer::StatInfo;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::MetadataRef;
use crate::ScalarExpr;

pub async fn filter_selectivity_sample(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    s_expr: &SExpr,
    sample_executor: Arc<dyn QuerySampleExecutor>,
) -> Result<Arc<StatInfo>> {
    // filter cardinality by sample will be called in `dphyp`, so we can ensure the filter is in complex query(contains not only one table)
    // Because it's meaningless for filter cardinality by sample in single table query.
    let child = s_expr.child(0)?;
    let child_rel_expr = RelExpr::with_s_expr(child);
    if let RelOperator::Scan(mut scan) = child.plan().clone() {
        let num_rows = scan
            .statistics
            .table_stats
            .as_ref()
            .and_then(|s| s.num_rows)
            .unwrap_or(0);

        // Calculate sample size (0.2% of total data)
        let sample_size = (num_rows as f64 * 0.002).ceil();
        let mut new_s_expr = s_expr.clone();
        // If the table is too small, we don't need to sample.
        if sample_size >= 10.0 {
            scan.sample = Some(Sample {
                sample_level: SampleLevel::ROW,
                sample_conf: SampleConfig::RowsNum(sample_size),
            });
            let new_child = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
            new_s_expr = s_expr.replace_children(vec![Arc::new(new_child)]);
            let collect_statistics_optimizer =
                CollectStatisticsOptimizer::new(ctx.clone(), metadata.clone());
            new_s_expr = collect_statistics_optimizer.run(&new_s_expr).await?;
        }

        new_s_expr = SExpr::create_unary(
            Arc::new(create_count_aggregate(AggregateMode::Partial).into()),
            Arc::new(new_s_expr),
        );
        new_s_expr = SExpr::create_unary(
            Arc::new(create_count_aggregate(AggregateMode::Final).into()),
            Arc::new(new_s_expr),
        );

        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
        let mut required = HashSet::new();
        required.insert(0);
        let plan = builder.build(&new_s_expr, required).await?;

        let result = sample_executor.execute_query(&plan).await?;
        if let Some(block) = result.first() {
            if let Some(count) = block.get_last_column().as_number() {
                if let Some(number_scalar) = count.index(0) {
                    // Compute and return selectivity
                    let selectivity = number_scalar.to_f64().to_f64().unwrap() / sample_size;
                    let stat_info = child_rel_expr.derive_cardinality()?;
                    let mut statistics = stat_info.statistics.clone();
                    let mut sb = SelectivityEstimator::new(
                        &mut statistics,
                        stat_info.cardinality,
                        HashSet::new(),
                    );
                    sb.update_other_statistic_by_selectivity(selectivity);
                    let stat_info = Arc::new(StatInfo {
                        cardinality: (selectivity * num_rows as f64).ceil(),
                        statistics,
                    });
                    *s_expr.stat_info.lock().unwrap() = Some(stat_info.clone());
                    return Ok(stat_info);
                }
            }
        }
    }
    Err(ErrorCode::Internal(
        "Failed to calculate filter selectivity by sample".to_string(),
    ))
}

fn create_count_aggregate(mode: AggregateMode) -> Aggregate {
    Aggregate {
        mode,
        group_items: vec![],
        aggregate_functions: vec![ScalarItem {
            scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                func_name: "count".to_string(),
                distinct: false,
                params: vec![],
                args: vec![],
                return_type: Box::new(DataType::Number(NumberDataType::UInt64)),
                display_name: "".to_string(),
            }),
            index: 0,
        }],
        from_distinct: false,
        limit: None,
        grouping_sets: None,
    }
}

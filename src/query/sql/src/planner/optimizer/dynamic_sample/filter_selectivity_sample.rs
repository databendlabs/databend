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
use databend_common_expression::types::NumberScalar;

use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::QuerySampleExecutor;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::Filter;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::MetadataRef;
use crate::ScalarExpr;

pub async fn filter_selectivity_sample(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    s_expr: &SExpr,
    sample_executor: Arc<dyn QuerySampleExecutor>,
) -> Result<f64> {
    // filter cardinality by sample will be called in `dphyp`, so we can ensure the filter is in complex query(contains not only one table)
    // Because it's meaningless for filter cardinality by sample in single table query.
    let filter = Filter::try_from(s_expr.plan().clone())?;
    let child = s_expr.child(0)?;
    if let RelOperator::Scan(mut scan) = child.plan().clone() {
        // Get the table's num_rows
        let num_rows = scan
            .statistics
            .table_stats
            .as_ref()
            .and_then(|s| s.num_rows)
            .unwrap_or(0);

        // Calculate sample size (0.2% of total data)
        let sample_size = (num_rows as f64 * 0.002).ceil();

        // 2. Construct sample field and add it to scan
        scan.sample = Some(Sample {
            sample_level: SampleLevel::ROW,
            sample_conf: SampleConfig::RowsNum(sample_size),
        });

        // Replace old scan in s_expr
        let new_child = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let mut new_s_expr = s_expr.replace_children(vec![Arc::new(new_child)]);

        // Wrap a count aggregate plan to original s_expr
        let count_agg = Aggregate {
            mode: AggregateMode::Initial,
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
                index: 0, // Assuming 0 is the correct index for the count result
            }],
            from_distinct: false,
            limit: None,
            grouping_sets: None,
        };
        new_s_expr = SExpr::create_unary(Arc::new(count_agg.into()), Arc::new(new_s_expr));

        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), true);
        let plan = builder.build(s_expr, HashSet::new()).await?;

        let result = sample_executor.execute_query(&plan).await?;
        if let Some(block) = result.first() {
            if let Some(count) = block.get_last_column().as_number() {
                if let Some(NumberScalar::UInt64(sampled_count)) = count.index(0) {
                    // Compute and return selectivity
                    let selectivity = sampled_count as f64 / sample_size as f64;
                    return Ok(selectivity);
                }
            }
        }
    }
    return Err(ErrorCode::Internal(
        "Failed to calculate filter selectivity by sample".to_string(),
    ));
}

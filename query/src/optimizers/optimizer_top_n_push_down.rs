// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;

use crate::optimizers::Optimizer;
use crate::sessions::DatabendQueryContextRef;

pub struct TopNPushDownOptimizer {}

// TODO: left/right outer join can also apply top_n push down. For example,
// 'select * from A left join B where A.id = B.id order by A.id limit 10;'
// The top_n can be pushed down to table A.
struct TopNPushDownImpl {
    before_group_by_schema: Option<DataSchemaRef>,
    limit: Option<usize>,
    order_by: Vec<Expression>,
}

impl PlanRewriter for TopNPushDownImpl {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be None",
            )),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        // With any aggregation in the plan pipeline, we clear the top n option.
        self.limit = None;

        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be Some",
            )),
            Some(schema_before_group_by) => {
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_final(schema_before_group_by, &new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_limit(&mut self, plan: &LimitPlan) -> Result<PlanNode> {
        let current_limit = self.limit;
        let current_order_by = self.order_by.clone();

        match plan.n {
            Some(limit) if limit > 0 => self.limit = Some(limit + plan.offset),
            _ => {}
        }

        let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
        let plan_node = PlanBuilder::from(&new_input)
            .limit_offset(plan.n, plan.offset)?
            .build();

        self.limit = current_limit; // recover back to previous state
        self.order_by = current_order_by;

        plan_node
    }

    fn rewrite_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        // push the limit and order_by down to read_source_plan
        if let Some(n) = self.limit {
            let mut new_plan = plan.clone();
            new_plan.push_downs = match &plan.push_downs {
                Some(extras) => {
                    let new_limit = if let Some(current_limit) = extras.limit {
                        n.min(current_limit)
                    } else {
                        n
                    };
                    Some(Extras {
                        projection: extras.projection.clone(),
                        filters: extras.filters.clone(),
                        limit: Some(new_limit),
                        order_by: self.order_by.clone(),
                    })
                }
                None => {
                    let mut extras = Extras::default();
                    extras.limit = Some(n);
                    Some(extras)
                }
            };
            return Ok(PlanNode::ReadSource(new_plan));
        }
        Ok(PlanNode::ReadSource(plan.clone()))
    }

    fn rewrite_subquery_plan(&mut self, subquery_plan: &PlanNode) -> Result<PlanNode> {
        let mut optimizer = TopNPushDownOptimizer {};
        optimizer.optimize(subquery_plan)
    }

    fn rewrite_sort(&mut self, plan: &SortPlan) -> Result<PlanNode> {
        if self.limit.is_some() {
            self.order_by = plan.order_by.clone();
        }
        let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
        let new_order_by = self.rewrite_exprs(&new_input.schema(), &plan.order_by)?;
        PlanBuilder::from(&new_input).sort(&new_order_by)?.build()
    }
}

impl TopNPushDownImpl {
    pub fn new() -> TopNPushDownImpl {
        TopNPushDownImpl {
            before_group_by_schema: None,
            limit: None,
            order_by: vec![],
        }
    }
}

impl Optimizer for TopNPushDownOptimizer {
    fn name(&self) -> &str {
        "TopNPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = TopNPushDownImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl TopNPushDownOptimizer {
    pub fn create(_ctx: DatabendQueryContextRef) -> TopNPushDownOptimizer {
        TopNPushDownOptimizer {}
    }
}

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
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::ReadDataSourcePlan;

use crate::catalogs::ToReadDataSourcePlan;
use crate::sessions::DatabendQueryContextRef;

pub struct PlanDoReadSource {
    ctx: DatabendQueryContextRef,
    before_group_by_schema: Option<DataSchemaRef>,
}

impl PlanDoReadSource {
    pub fn create(ctx: DatabendQueryContextRef) -> PlanDoReadSource {
        PlanDoReadSource {
            ctx,
            before_group_by_schema: None,
        }
    }
}

impl PlanRewriter for PlanDoReadSource {
    fn rewrite_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        let context = self.ctx.clone();
        let table = context.build_table_from_source_plan(plan)?;
        futures::executor::block_on(async move {
            let plan = table
                .read_plan(context.clone(), plan.push_downs.clone())
                .await?;
            Ok(PlanNode::ReadSource(plan))
        })
    }

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
}

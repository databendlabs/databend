//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_meta_types::TableInfo;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SinkPlan;
use common_planners::StagePlan;
use common_streams::SendableDataBlockStream;

use crate::interpreters::plan_schedulers;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct InsertWithPlan<'a> {
    pub ctx: &'a Arc<QueryContext>,
    pub schema: &'a Arc<DataSchema>,
    pub plan_node: &'a PlanNode,
}

impl<'a> InsertWithPlan<'a> {
    pub fn new(
        ctx: &'a Arc<QueryContext>,
        schema: &'a Arc<DataSchema>,
        plan_node: &'a PlanNode,
    ) -> Self {
        Self {
            ctx,
            schema,
            plan_node,
        }
    }

    pub async fn execute(
        &self,
        table: &dyn Table,
    ) -> common_exception::Result<SendableDataBlockStream> {
        if let PlanNode::Select(sel) = self.plan_node {
            let optimized_plan = self.rewrite_plan(sel, table.get_table_info())?;
            plan_schedulers::schedule_query(self.ctx, &optimized_plan).await
        } else {
            Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Unsupported select query plan for insert_into interpreter, {}",
                self.plan_node.name()
            )))
        }
    }

    fn rewrite_plan(
        &self,
        select_plan: &SelectPlan,
        table_info: &TableInfo,
    ) -> common_exception::Result<PlanNode> {
        let cast_schema = if self.check_schema_cast(select_plan)? {
            Some(self.schema.clone())
        } else {
            None
        };

        // optimize and rewrite the SelectPlan.input
        let optimized_plan = plan_schedulers::apply_plan_rewrite(
            Optimizers::create(self.ctx.clone()),
            &select_plan.input,
        )?;

        // rewrite the optimized the plan
        let rewritten_plan = match optimized_plan {
            // if it is a StagePlan Node, we insert the a SinkPlan in between the Stage and Stage.input
            // i.e.
            //    StagePlan <~ PlanNodeA  => StagePlan <~ Sink <~ PlanNodeA
            PlanNode::Stage(r) => {
                let prev_input = r.input.clone();
                let sink = PlanNode::Sink(SinkPlan {
                    table_info: table_info.clone(),
                    input: prev_input,
                    cast_schema,
                });
                PlanNode::Stage(StagePlan {
                    kind: r.kind,
                    input: Arc::new(sink),
                    scatters_expr: r.scatters_expr,
                })
            }
            // otherwise, we just prepend a SinkPlan
            // i.e.
            //    node <~ PlanNodeA  => Sink<~ node <~ PlanNodeA
            node => PlanNode::Sink(SinkPlan {
                table_info: table_info.clone(),
                input: Arc::new(node),
                cast_schema,
            }),
        };
        Ok(rewritten_plan)
    }

    fn check_schema_cast(&self, select_plan: &SelectPlan) -> common_exception::Result<bool> {
        let output_schema = self.schema;
        let select_schema = select_plan.schema();

        // validate schema
        if select_schema.fields().len() < output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(
                "Fields in select statement is less than expected",
            ));
        }

        // check if cast needed
        let cast_needed = select_schema != *output_schema;
        Ok(cast_needed)
    }
}

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

use std::io::Cursor;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SinkPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_streams::ValueSource;
use futures::TryStreamExt;

use crate::catalogs::Table;
use crate::interpreters::interpreter_select::Scheduled;
use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::utils::apply_plan_rewrite;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::DatabendQueryContextRef;

pub struct InsertIntoInterpreter {
    ctx: DatabendQueryContextRef,
    plan: InsertIntoPlan,
}

impl InsertIntoInterpreter {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        plan: InsertIntoPlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertIntoInterpreter { ctx, plan }))
    }

    async fn insert_with_select_plan(
        &self,
        plan_node: &PlanNode,
        table: &dyn Table,
    ) -> Result<Vec<DataBlock>> {
        if let PlanNode::Select(sel) = plan_node {
            // check if schema match
            let output_schema = self.plan.schema();
            let select_schema = sel.schema();
            if select_schema.fields().len() < output_schema.fields().len() {
                return Err(ErrorCode::BadArguments(
                    "Fields in select statement is less than expected",
                ));
            }
            let upstream_schema = if select_schema == output_schema {
                None
            } else {
                Some(select_schema)
            };

            let mut scheduled = Scheduled::new();
            self.schedule_query(&mut scheduled, sel, table.get_table_info(), upstream_schema)
                .await
        } else {
            Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Unsupported select query plan for insert_into interpreter, {}",
                plan_node.name()
            )))
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertIntoInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let database = &self.plan.db_name;
        let table = &self.plan.tbl_name;
        let table = self.ctx.get_table(database, table).await?;

        let append_operations = if let Some(plan_node) = &self.plan.select_plan {
            self.insert_with_select_plan(plan_node.as_ref(), table.as_ref())
                .await?
        } else {
            let input_stream = if self.plan.values_opt.is_some() {
                let values = self.plan.values_opt.clone().take().ok_or_else(|| {
                    ErrorCode::EmptyData("values of insert plan not exist or consumed")
                })?;
                let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
                let values_source =
                    ValueSource::new(Cursor::new(values), self.plan.schema(), block_size);
                let stream_source = SourceStream::new(Box::new(values_source));
                stream_source.execute().await
            } else {
                input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))
            }?;
            table
                .append_data(self.ctx.clone(), input_stream)
                .await?
                .try_collect()
                .await?
        };
        table.commit(self.ctx.clone(), append_operations).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}

impl InsertIntoInterpreter {
    // TODO duplicated code!
    async fn schedule_query(
        &self,
        scheduled: &mut Scheduled,
        select_plan: &SelectPlan,
        table_info: &TableInfo,
        upstream_schema: Option<DataSchemaRef>,
    ) -> Result<Vec<DataBlock>> {
        // This is almost the same of SelectInterpreter::schedule_query, with some slight tweaks

        // As select interpreter does
        // we optimize and rewrite the select_plan.input first
        let optimized_plan = apply_plan_rewrite(
            self.ctx.clone(),
            Optimizers::create(self.ctx.clone()),
            select_plan.input.as_ref(),
        )?;

        // here, we wrapped the optimized/rewritten plan in a SinkPlan
        let sink_plan = PlanNode::Sink(SinkPlan {
            table_info: table_info.clone(),
            input: Arc::new(optimized_plan.clone()),
            upstream_schema,
        });

        // it might be better, if the above logics could be encapsulated in PipelineBuilder

        // following logics are the same
        let scheduler = PlanScheduler::try_create(self.ctx.clone())?;
        let scheduled_tasks = scheduler.reschedule(&sink_plan)?;
        let remote_stage_actions = scheduled_tasks.get_tasks()?;

        let config = self.ctx.get_config();
        let cluster = self.ctx.get_cluster();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        for (node, action) in remote_stage_actions {
            let mut flight_client = cluster.create_node_conn(&node.id, &config).await?;
            let executing_action = flight_client.execute_action(action.clone(), timeout);

            executing_action.await?;
            scheduled.insert(node.id.clone(), node.clone());
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
        let inserts = in_local_pipeline.execute().await?;
        inserts.try_collect::<Vec<_>>().await
    }
}

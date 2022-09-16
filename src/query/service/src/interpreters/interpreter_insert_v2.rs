// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use parking_lot::Mutex;

use super::interpreter_common::append2table;
use super::plan_schedulers::build_schedule_pipepline;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::BlocksSource;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::DistributedInsertSelect;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::executor::PipelineBuilder;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;

pub struct InsertInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: Insert,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
    async_insert: bool,
}

impl InsertInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Insert,
        async_insert: bool,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreterV2 {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
            async_insert,
        }))
    }

    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

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

#[async_trait::async_trait]
impl Interpreter for InsertInterpreterV2 {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let settings = self.ctx.get_settings();
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut build_res = PipelineBuildResult::create();
        let mut builder = SourcePipeBuilder::create();

        if self.async_insert {
            build_res.main_pipeline.add_pipe(
                ((*self.source_pipe_builder.lock()).clone())
                    .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                    .finalize(),
            );
        } else {
            match &self.plan.source {
                InsertInputSource::Values(values) => {
                    let blocks =
                        Arc::new(Mutex::new(VecDeque::from_iter(vec![values.block.clone()])));

                    for _index in 0..settings.get_max_threads()? {
                        let output = OutputPort::create();
                        builder.add_source(
                            output.clone(),
                            BlocksSource::create(self.ctx.clone(), output.clone(), blocks.clone())?,
                        );
                    }
                    build_res.main_pipeline.add_pipe(builder.finalize());
                }
                InsertInputSource::StreamingWithFormat(_) => {
                    build_res.main_pipeline.add_pipe(
                        ((*self.source_pipe_builder.lock()).clone())
                            .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                            .finalize(),
                    );
                }
                InsertInputSource::SelectPlan(plan) => {
                    let table1 = table.clone();
                    let (mut select_plan, select_column_bindings) = match plan.as_ref() {
                        Plan::Query {
                            s_expr,
                            metadata,
                            bind_context,
                            ..
                        } => {
                            let builder1 =
                                PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
                            (builder1.build(s_expr).await?, bind_context.columns.clone())
                        }
                        _ => unreachable!(),
                    };

                    table1.get_table_info();
                    let catalog = self.plan.catalog.clone();
                    let is_distributed_plan = select_plan.is_distributed_plan();

                    let insert_select_plan = match select_plan {
                        PhysicalPlan::Exchange(ref mut exchange) => {
                            // insert can be dispatched to different nodes
                            let input = exchange.input.clone();
                            exchange.input = Box::new(PhysicalPlan::DistributedInsertSelect(
                                Box::new(DistributedInsertSelect {
                                    input,
                                    catalog,
                                    table_info: table1.get_table_info().clone(),
                                    select_schema: plan.schema(),
                                    select_column_bindings,
                                    insert_schema: self.plan.schema(),
                                    cast_needed: self.check_schema_cast(plan)?,
                                }),
                            ));
                            select_plan
                        }
                        other_plan => {
                            // insert should wait until all nodes finished
                            PhysicalPlan::DistributedInsertSelect(Box::new(
                                DistributedInsertSelect {
                                    input: Box::new(other_plan),
                                    catalog,
                                    table_info: table1.get_table_info().clone(),
                                    select_schema: plan.schema(),
                                    select_column_bindings,
                                    insert_schema: self.plan.schema(),
                                    cast_needed: self.check_schema_cast(plan)?,
                                },
                            ))
                        }
                    };

                    let mut build_res = match is_distributed_plan {
                        true => {
                            build_schedule_pipepline(self.ctx.clone(), &insert_select_plan).await
                        }
                        false => {
                            PipelineBuilder::create(self.ctx.clone()).finalize(&insert_select_plan)
                        }
                    }?;

                    let ctx = self.ctx.clone();
                    let overwrite = self.plan.overwrite;
                    build_res.main_pipeline.set_on_finished(move |may_error| {
                        // capture out variable
                        let overwrite = overwrite;
                        let ctx = ctx.clone();
                        let table = table.clone();

                        if may_error.is_none() {
                            let append_entries = ctx.consume_precommit_blocks();
                            // We must put the commit operation to global runtime, which will avoid the "dispatch dropped without returning error" in tower
                            let catalog_name = ctx.get_current_catalog();
                            let commit_handle = GlobalIORuntime::instance().spawn(async move {
                                table
                                    .commit_insertion(ctx, &catalog_name, append_entries, overwrite)
                                    .await
                            });

                            return match futures::executor::block_on(commit_handle) {
                                Ok(Ok(_)) => Ok(()),
                                Ok(Err(error)) => Err(error),
                                Err(cause) => Err(ErrorCode::PanicError(format!(
                                    "Maybe panic while in commit insert. {}",
                                    cause
                                ))),
                            };
                        }

                        Err(may_error.as_ref().unwrap().clone())
                    });

                    return Ok(build_res);
                }
            };
        }

        append2table(
            self.ctx.clone(),
            table.clone(),
            plan.schema(),
            &mut build_res,
            self.plan.overwrite,
            true,
        )?;

        Ok(build_res)
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}

// Copyright 2021 Datafuse Labs.
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

use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use parking_lot::Mutex;

use super::commit2table;
use super::interpreter_common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::BlocksSource;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertPlan,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
    async_insert: bool,
}

impl InsertInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: InsertPlan,
        async_insert: bool,
    ) -> Result<Self> {
        Ok(InsertInterpreter {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
            async_insert,
        })
    }

    fn check_schema_cast(&self, plan_node: &PlanNode) -> common_exception::Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan_node.schema();

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
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let settings = self.ctx.get_settings();
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let mut build_res = self.create_new_pipeline().await?;
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
                    let select_interpreter =
                        SelectInterpreter::try_create(self.ctx.clone(), SelectPlan {
                            input: Arc::new((**plan).clone()),
                        })?;
                    build_res = select_interpreter.create_new_pipeline().await?;

                    if self.check_schema_cast(plan)? {
                        let mut functions = Vec::with_capacity(self.plan.schema().fields().len());
                        for (target_field, original_field) in self
                            .plan
                            .schema()
                            .fields()
                            .iter()
                            .zip(plan.schema().fields().iter())
                        {
                            let target_type_name = target_field.data_type().name();
                            let from_type = original_field.data_type().clone();
                            let cast_function =
                                CastFunction::create("cast", &target_type_name, from_type).unwrap();
                            functions.push(cast_function);
                        }
                        let func_ctx = self.ctx.try_get_function_context()?;
                        build_res.main_pipeline.add_transform(
                            |transform_input_port, transform_output_port| {
                                TransformCastSchema::try_create(
                                    transform_input_port,
                                    transform_output_port,
                                    self.plan.schema(),
                                    functions.clone(),
                                    func_ctx.clone(),
                                )
                            },
                        )?;
                    }
                }
            };
        }

        append2table(self.ctx.clone(), table.clone(), plan.schema(), build_res)?;
        commit2table(self.ctx.clone(), table.clone(), plan.overwrite).await?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }

    async fn create_new_pipeline(&self) -> Result<PipelineBuildResult> {
        let insert_pipeline = Pipeline::create();
        Ok(PipelineBuildResult {
            main_pipeline: insert_pipeline,
            sources_pipelines: vec![],
        })
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}

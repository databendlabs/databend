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

use chrono_tz::Tz;
use common_base::base::TrySpawn;
use common_base::infallible::Mutex;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionContext;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::BlocksSource;
use crate::pipelines::new::processors::TransformAddOn;
use crate::pipelines::new::processors::TransformCastSchema;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
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

    async fn execute_new(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let settings = self.ctx.get_settings();
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut pipeline = self.create_new_pipeline()?;
        let mut builder = SourcePipeBuilder::create();

        if self.async_insert {
            pipeline.add_pipe(
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
                    pipeline.add_pipe(builder.finalize());
                }
                InsertInputSource::StreamingWithFormat(_) => {
                    pipeline.add_pipe(
                        ((*self.source_pipe_builder.lock()).clone())
                            .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                            .finalize(),
                    );
                }
                InsertInputSource::SelectPlan(plan) => {
                    let select_interpreter = match &**plan {
                        Plan::Query {
                            s_expr,
                            metadata,
                            bind_context,
                        } => SelectInterpreterV2::try_create(
                            self.ctx.clone(),
                            *bind_context.clone(),
                            s_expr.clone(),
                            metadata.clone(),
                        ),
                        _ => unreachable!(),
                    };

                    pipeline = select_interpreter?.create_new_pipeline()?;

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
                        let tz = self.ctx.get_settings().get_timezone()?;
                        let tz = String::from_utf8(tz).map_err(|_| {
                            ErrorCode::LogicalError(
                                "Timezone has been checked and should be valid.",
                            )
                        })?;
                        let tz = tz.parse::<Tz>().map_err(|_| {
                            ErrorCode::InvalidTimezone(
                                "Timezone has been checked and should be valid",
                            )
                        })?;
                        let func_ctx = FunctionContext { tz };
                        pipeline.add_transform(|transform_input_port, transform_output_port| {
                            TransformCastSchema::try_create(
                                transform_input_port,
                                transform_output_port,
                                self.plan.schema(),
                                functions.clone(),
                                func_ctx.clone(),
                            )
                        })?;
                    }
                }
            };
        }

        let need_fill_missing_columns = table.schema() != plan.schema();
        if need_fill_missing_columns {
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformAddOn::try_create(
                    transform_input_port,
                    transform_output_port,
                    self.plan.schema(),
                    table.schema(),
                    self.ctx.clone(),
                )
            })?;
        }

        table.append2(self.ctx.clone(), &mut pipeline)?;

        let async_runtime = self.ctx.get_storage_runtime();

        pipeline.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);
        let executor = PipelineCompleteExecutor::try_create(async_runtime, pipeline)?;
        executor.execute()?;
        drop(executor);

        let overwrite = self.plan.overwrite;
        let catalog_name = self.plan.catalog.clone();
        let context = self.ctx.clone();
        let append_entries = self.ctx.consume_precommit_blocks();

        // We must put the commit operation to global runtime, which will avoid the "dispatch dropped without returning error" in tower
        let handler = self.ctx.get_storage_runtime().spawn(async move {
            table
                .commit_insertion(context, &catalog_name, append_entries, overwrite)
                .await
        });

        match handler.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(cause)) => Err(cause),
            Err(cause) => Err(ErrorCode::PanicError(format!(
                "Maybe panic while in commit insert. {}",
                cause
            ))),
        }?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }

    fn check_schema_cast(&self, plan: &Plan) -> common_exception::Result<bool> {
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

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        self.execute_new(input_stream).await
    }

    fn create_new_pipeline(&self) -> Result<NewPipeline> {
        let insert_pipeline = NewPipeline::create();
        Ok(insert_pipeline)
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}

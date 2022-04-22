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

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionContext;
use common_infallible::Mutex;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;

use crate::interpreters::interpreter_insert_with_stream::InsertWithStream;
use crate::interpreters::plan_schedulers::InsertWithPlan;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::BlocksSource;
use crate::pipelines::new::processors::TransformAddOn;
use crate::pipelines::new::processors::TransformCastSchema;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::pipelines::transforms::AddOnStream;
use crate::sessions::QueryContext;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertPlan,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
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
            .get_table(&plan.catalog_name, &plan.database_name, &plan.table_name)
            .await?;

        let mut pipeline = self.create_new_pipeline()?;
        let mut builder = SourcePipeBuilder::create();
        let mut need_cast_schema = false;
        match &self.plan.source {
            InsertInputSource::Values(values) => {
                let blocks = Arc::new(Mutex::new(VecDeque::from_iter(vec![values.block.clone()])));

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
                need_cast_schema = self.check_schema_cast(plan)?;
                let select_interpreter =
                    SelectInterpreter::try_create(self.ctx.clone(), SelectPlan {
                        input: Arc::new((**plan).clone()),
                    })?;
                pipeline = select_interpreter.create_new_pipeline()?;
            }
        };

        // cast schema
        if need_cast_schema {
            let mut functions = Vec::with_capacity(self.plan.schema().fields().len());
            for field in self.plan.schema().fields() {
                let name = format!("{:?}", field.data_type());
                let cast_function = CastFunction::create("cast", &name).unwrap();
                functions.push(cast_function);
            }
            let tz = self.ctx.get_settings().get_timezone()?;
            let tz = String::from_utf8(tz).map_err(|_| {
                ErrorCode::LogicalError("Timezone has been checked and should be valid.")
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

        let append_entries = self.ctx.consume_precommit_blocks();
        table
            .commit_insertion(self.ctx.clone(), append_entries, self.plan.overwrite)
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
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

    async fn execute(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let settings = self.ctx.get_settings();

        // Use insert in new processor
        if settings.get_enable_new_processor_framework()? != 0 && self.ctx.get_cluster().is_empty()
        {
            return self.execute_new(input_stream).await;
        }

        let plan = &self.plan;
        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Table(
                    plan.catalog_name.clone(),
                    plan.database_name.clone(),
                    plan.table_name.clone(),
                ),
                UserPrivilegeType::Insert,
            )
            .await?;

        let table = self
            .ctx
            .get_table(&plan.catalog_name, &plan.database_name, &plan.table_name)
            .await?;

        let need_fill_missing_columns = table.schema() != self.plan.schema();

        let append_logs = match &self.plan.source {
            InsertInputSource::SelectPlan(plan_node) => {
                let with_plan = InsertWithPlan::new(&self.ctx, &self.plan.schema, plan_node);
                with_plan.execute(table.as_ref()).await
            }

            InsertInputSource::Values(values) => {
                let stream: SendableDataBlockStream =
                    Box::pin(futures::stream::iter(vec![Ok(values.block.clone())]));
                let stream = if need_fill_missing_columns {
                    Box::pin(AddOnStream::try_create(
                        stream,
                        self.plan.schema(),
                        table.schema(),
                        self.ctx.clone(),
                    )?)
                } else {
                    stream
                };

                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }

            InsertInputSource::StreamingWithFormat(_) => {
                let stream = input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))?;

                let stream = if need_fill_missing_columns {
                    Box::pin(AddOnStream::try_create(
                        stream,
                        self.plan.schema(),
                        table.schema(),
                        self.ctx.clone(),
                    )?)
                } else {
                    stream
                };

                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }
        }?;
        // feed back the append operation logs to table
        table
            .commit_insertion(
                self.ctx.clone(),
                append_logs.try_collect().await?,
                self.plan.overwrite,
            )
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }

    fn create_new_pipeline(&self) -> Result<NewPipeline> {
        let new_pipeline = NewPipeline::create();
        Ok(new_pipeline)
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}

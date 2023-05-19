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

use std::sync::Arc;
use std::time::Instant;

use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::StageAttachment;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_meta_app::principal::FileFormatOptionsAst;
use common_pipeline_sources::AsyncSourcer;
use common_sql::binder::parse_stage_location;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::plans::InsertInputSource;
use common_sql::plans::Plan;
use common_sql::plans::Replace;
use common_sql::NameResolutionContext;
use common_storage::StageFilesInfo;
use common_storages_stage::StageTable;
use tracing::info;

use crate::interpreters::common::prepared_values;
use crate::interpreters::common::try_purge_files;
use crate::interpreters::interpreter_insert::ValueSource;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct ReplaceInterpreter {
    ctx: Arc<QueryContext>,
    plan: Replace,
}

impl ReplaceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Replace) -> Result<InterpreterPtr> {
        Ok(Arc::new(ReplaceInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ReplaceInterpreter {
    fn name(&self) -> &str {
        "ReplaceIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        self.check_on_conflicts()?;

        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut pipeline = self
            .connect_input_source(self.ctx.clone(), &self.plan.source, self.plan.schema())
            .await?;

        pipeline
            .main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformResortAddOn::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    self.plan.schema(),
                    table.clone(),
                )
            })?;

        let on_conflict_fields = plan.on_conflict_fields.clone();
        table
            .replace_into(
                self.ctx.clone(),
                &mut pipeline.main_pipeline,
                on_conflict_fields,
            )
            .await?;
        Ok(pipeline)
    }
}

impl ReplaceInterpreter {
    fn check_on_conflicts(&self) -> Result<()> {
        if self.plan.on_conflict_fields.is_empty() {
            Err(ErrorCode::BadArguments(
                "at least one column must be specified in the replace into .. on [conflict] statement",
            ))
        } else {
            Ok(())
        }
    }
    #[async_backtrace::framed]
    async fn connect_input_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        source: &'a InsertInputSource,
        schema: DataSchemaRef,
    ) -> Result<PipelineBuildResult> {
        match source {
            InsertInputSource::Values(data) => {
                self.connect_value_source(ctx.clone(), schema.clone(), data)
            }

            InsertInputSource::SelectPlan(plan) => {
                self.connect_query_plan_source(ctx.clone(), schema.clone(), plan)
                    .await
            }
            InsertInputSource::Stage(attach) => {
                info!("insert: from stage with attachment {:?}", attach);
                self.connect_stage_source(ctx.clone(), schema.clone(), attach)
                    .await
            }
            _ => Err(ErrorCode::Unimplemented(
                "input source other than literal VALUES and sub queries are NOT supported yet.",
            )),
        }
    }

    fn connect_value_source(
        &self,
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        value_data: &str,
    ) -> Result<PipelineBuildResult> {
        let mut build_res = PipelineBuildResult::create();
        let settings = ctx.get_settings();
        build_res.main_pipeline.add_source(
            |output| {
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let inner = ValueSource::new(
                    value_data.to_string(),
                    ctx.clone(),
                    name_resolution_ctx,
                    schema.clone(),
                );
                AsyncSourcer::create(ctx.clone(), output, inner)
            },
            1,
        )?;
        Ok(build_res)
    }

    #[async_backtrace::framed]
    async fn connect_stage_source(
        &self,
        ctx: Arc<QueryContext>,
        source_schema: DataSchemaRef,
        attachment: &Arc<StageAttachment>,
    ) -> Result<PipelineBuildResult> {
        let start = Instant::now();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let catalog_name = self.plan.catalog.clone();

        // find the desired schema for stage attach data to insert
        let (attachment_data_schema, const_columns) = if attachment.values_str.is_empty() {
            (source_schema.clone(), vec![])
        } else {
            prepared_values(&ctx, &source_schema, attachment).await?
        };
        let (mut stage_info, path) = parse_stage_location(&table_ctx, &attachment.location).await?;
        if let Some(ref options) = attachment.file_format_options {
            stage_info.file_format_params = FileFormatOptionsAst {
                options: options.clone(),
            }
            .try_into()?;
        }
        if let Some(ref options) = attachment.copy_options {
            stage_info.copy_options.apply(options, true)?;
        }

        let attachment_table_schema = infer_table_schema(&attachment_data_schema)?;
        let mut stage_table_info = StageTableInfo {
            schema: attachment_table_schema,
            stage_info,
            files_info: StageFilesInfo {
                path: path.to_string(),
                files: None,
                pattern: None,
            },
            files_to_copy: None,
        };
        let all_source_files = StageTable::list_files(&stage_table_info, None).await?;
        info!(
            "replace: read all stage attachment files finished: {}, elapsed:{}",
            all_source_files.len(),
            start.elapsed().as_secs()
        );

        stage_table_info.files_to_copy = Some(all_source_files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(ctx.clone(), catalog_name, None, None)
                .await?
        };
        let mut build_res = PipelineBuildResult::create();

        let pipeline = &mut build_res.main_pipeline;
        stage_table.read_data(table_ctx, &read_source_plan, pipeline)?;
        if !const_columns.is_empty() {
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformAddConstColumns::try_create(
                    ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    attachment_data_schema.clone(),
                    source_schema.clone(),
                    const_columns.clone(),
                )
            })?;
        }

        // Garbage collection for stage attachment files
        // Note it does not GUARANTEE stage files would be purged completely
        let stage_info_clone = stage_table_info.stage_info.clone();
        pipeline.set_on_finished(move |may_error| {
            // capture out variable
            let ctx = ctx.clone();
            let stage_info = stage_info_clone.clone();
            let all_source_files = all_source_files.clone();

            match may_error {
                Some(error) => {
                    tracing::error!("replace stage file error: {}", error);
                    Err(may_error.as_ref().unwrap().clone())
                }
                None => GlobalIORuntime::instance().block_on(async move {
                    if stage_info.copy_options.purge {
                        info!(
                            "replace: try to purge files:{}, elapsed:{}",
                            all_source_files.len(),
                            start.elapsed().as_secs()
                        );
                        try_purge_files(ctx.clone(), &stage_info, &all_source_files).await;
                    }

                    Ok(())
                }),
            }
        });
        Ok(build_res)
    }

    #[async_backtrace::framed]
    async fn connect_query_plan_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        self_schema: DataSchemaRef,
        query_plan: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query_plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreter::try_create(
            ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        let mut build_res = select_interpreter.execute2().await?;

        let select_schema = query_plan.schema();
        let target_schema = self_schema;
        if self.check_schema_cast(query_plan)? {
            let func_ctx = ctx.get_function_context()?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        select_schema.clone(),
                        target_schema.clone(),
                        func_ctx.clone(),
                    )
                },
            )?;
        }

        Ok(build_res)
    }

    // TODO duplicated
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
        let cast_needed = select_schema != DataSchema::from(output_schema.as_ref()).into();
        Ok(cast_needed)
    }
}

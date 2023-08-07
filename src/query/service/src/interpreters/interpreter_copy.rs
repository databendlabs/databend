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

use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::BlockThresholds;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_meta_app::principal::StageInfo;
use common_pipeline_core::Pipeline;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::executor::CopyIntoTableFromQuery;
use common_sql::executor::DistributedCopyIntoTableFromStage;
use common_sql::executor::Exchange;
use common_sql::executor::FragmentKind;
use common_sql::executor::PhysicalPlan;
use common_sql::plans::CopyIntoTableMode;
use common_sql::plans::CopyIntoTablePlan;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_stage::StageTable;
use log::debug;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::hook_compact;
use crate::interpreters::common::CompactHookTraceCtx;
use crate::interpreters::common::CompactTargetTableDescription;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::builders::build_append2table_with_commit_pipeline;
use crate::pipelines::builders::build_append_data_pipeline;
use crate::pipelines::builders::build_commit_data_pipeline;
use crate::pipelines::builders::set_copy_on_finished;
use crate::pipelines::builders::CopyPlanType;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_distributed_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlan;
use crate::sql::plans::Plan;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    /// Create a CopyInterpreter with context and [`CopyPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<Self> {
        Ok(CopyInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(&self, query: &Plan) -> Result<(SelectInterpreter, DataSchemaRef)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query {
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
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        // Building data schema from bind_context columns
        // TODO(leiyskey): Extract the following logic as new API of BindContext.
        let fields = bind_context
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        let data_schema = DataSchemaRefExt::create(fields);

        Ok((select_interpreter, data_schema))
    }

    /// Build a pipeline for local copy into stage.
    #[async_backtrace::framed]
    async fn build_local_copy_into_stage_pipeline(
        &self,
        stage: &StageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (select_interpreter, data_schema) = self.build_query(query).await?;
        let plan = select_interpreter.build_physical_plan().await?;
        let mut build_res = select_interpreter.build_pipeline(plan).await?;
        let table_schema = infer_table_schema(&data_schema)?;
        let stage_table_info = StageTableInfo {
            schema: table_schema,
            stage_info: stage.clone(),
            files_info: StageFilesInfo {
                path: path.to_string(),
                files: None,
                pattern: None,
            },
            files_to_copy: None,
            is_select: false,
        };
        let to_table = StageTable::try_create(stage_table_info)?;
        build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            to_table,
            data_schema,
            None,
            false,
            AppendMode::Normal,
        )?;
        Ok(build_res)
    }

    fn set_status(&self, status: &str) {
        self.ctx.set_status_info(status);
        info!("{}", status);
    }

    #[async_backtrace::framed]
    async fn try_transform_copy_plan_from_local_to_distributed(
        &self,
        plan: &CopyIntoTablePlan,
    ) -> Result<Option<CopyPlanType>> {
        let ctx = self.ctx.clone();
        let to_table = ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;
        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        let files = plan.collect_files(&table_ctx).await?;
        if files.is_empty() {
            return Ok(None);
        }
        let mut stage_table_info = plan.stage_table_info.clone();
        stage_table_info.files_to_copy = Some(files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        if plan.query.is_none() {
            let read_source_plan = {
                stage_table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        plan.catalog_info.catalog_name().to_string(),
                        None,
                        None,
                        false,
                    )
                    .await?
            };

            if read_source_plan.parts.len() <= 1 {
                return Ok(None);
            }
            Ok(Some(CopyPlanType::DistributedCopyIntoTableFromStage(
                DistributedCopyIntoTableFromStage {
                    // TODO(leiysky): we reuse the id of exchange here,
                    // which is not correct. We should generate a new id for insert.
                    plan_id: 0,
                    catalog_info: plan.catalog_info.clone(),
                    database_name: plan.database_name.clone(),
                    table_name: plan.table_name.clone(),
                    required_values_schema: plan.required_values_schema.clone(),
                    values_consts: plan.values_consts.clone(),
                    required_source_schema: plan.required_source_schema.clone(),
                    stage_table_info: plan.stage_table_info.clone(),
                    source: Box::new(read_source_plan),
                    files,
                    table_info: to_table.get_table_info().clone(),
                    force: plan.force,
                    write_mode: plan.write_mode,
                    thresholds: to_table.get_block_thresholds(),
                    validation_mode: plan.validation_mode.clone(),
                },
            )))
        } else {
            // plan query must exist, we can use unwarp directly.
            let (select_interpreter, query_source_schema) =
                self.build_query(plan.query.as_ref().unwrap()).await?;
            let plan_query = select_interpreter.build_physical_plan().await?;
            Ok(Some(CopyPlanType::CopyIntoTableFromQuery(
                CopyIntoTableFromQuery {
                    // add exchange plan node to enable distributed
                    // TODO(leiysky): we reuse the id of exchange here,
                    // which is not correct. We should generate a new id
                    plan_id: 0,
                    ignore_result: select_interpreter.get_ignore_result(),
                    catalog_info: plan.catalog_info.clone(),
                    database_name: plan.database_name.clone(),
                    table_name: plan.table_name.clone(),
                    required_source_schema: plan.required_source_schema.clone(),
                    values_consts: plan.values_consts.clone(),
                    required_values_schema: plan.required_values_schema.clone(),
                    result_columns: select_interpreter.get_result_columns(),
                    query_source_schema,
                    write_mode: plan.write_mode,
                    validation_mode: plan.validation_mode.clone(),
                    force: plan.force,
                    stage_table_info: plan.stage_table_info.clone(),
                    local_node_id: self.ctx.get_cluster().local_id.clone(),
                    input: Box::new(plan_query),
                    files: plan.collect_files(&table_ctx).await?,
                    table_info: to_table.get_table_info().clone(),
                },
            )))
        }
    }

    #[async_backtrace::framed]
    async fn build_read_stage_table_data_pipeline(
        &self,
        pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        block_thresholds: BlockThresholds,
        files: Vec<StageFileInfo>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();

        let mut stage_table_info = plan.stage_table_info.clone();
        stage_table_info.files_to_copy = Some(files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(
                    ctx.clone(),
                    plan.catalog_info.catalog_name().to_string(),
                    None,
                    None,
                    false,
                )
                .await?
        };

        stage_table.set_block_thresholds(block_thresholds);
        stage_table.read_data(table_ctx, &read_source_plan, pipeline)?;

        Ok(())
    }

    /// Build a COPY pipeline in standalone mode.
    #[async_backtrace::framed]
    async fn build_local_copy_into_table_pipeline(
        &self,
        plan: &CopyIntoTablePlan,
    ) -> Result<PipelineBuildResult> {
        let catalog = plan.catalog_info.catalog_name();
        let database = plan.database_name.as_str();
        let table = plan.table_name.as_str();

        let ctx = self.ctx.clone();
        let to_table = ctx.get_table(catalog, database, table).await?;

        let mut build_res;
        let source_schema;
        let files;

        match &plan.query {
            None => {
                let table_ctx: Arc<dyn TableContext> = self.ctx.clone();

                files = plan.collect_files(&table_ctx).await?;
                source_schema = plan.required_source_schema.clone();
                build_res = PipelineBuildResult::create();
                if !files.is_empty() {
                    self.build_read_stage_table_data_pipeline(
                        &mut build_res.main_pipeline,
                        plan,
                        to_table.get_block_thresholds(),
                        files.clone(),
                    )
                    .await?;
                } else {
                    return Ok(build_res);
                }
            }
            Some(query) => {
                files = plan
                    .stage_table_info
                    .files_to_copy
                    .clone()
                    .ok_or(ErrorCode::Internal("files_to_copy should not be None"))?;

                let (select_interpreter, query_source_schema) = self.build_query(query).await?;
                let plan = select_interpreter.build_physical_plan().await?;
                build_res = select_interpreter.build_pipeline(plan).await?;
                source_schema = query_source_schema;
            }
        }

        let file_sizes: u64 = files.iter().map(|f| f.size).sum();

        // Append data.
        {
            self.set_status(&format!(
                "Copy begin to append data: {} files, size_in_bytes:{} into table",
                files.len(),
                file_sizes
            ));

            let start = Instant::now();
            build_append_data_pipeline(
                ctx.clone(),
                &mut build_res.main_pipeline,
                CopyPlanType::CopyIntoTablePlanOption(plan.clone()),
                source_schema,
                to_table.clone(),
            )?;

            // Perf
            {
                self.set_status(&format!(
                    "Copy append data finished, cost:{} secs",
                    start.elapsed().as_secs()
                ));
            }
        }

        // Commit data.
        {
            // if it's replace mode, don't commit, because COPY is the source of replace.
            match plan.write_mode {
                CopyIntoTableMode::Replace => set_copy_on_finished(
                    ctx,
                    files,
                    plan.stage_table_info.stage_info.copy_options.purge,
                    plan.stage_table_info.stage_info.clone(),
                    &mut build_res.main_pipeline,
                )?,
                _ => {
                    // commit.
                    build_commit_data_pipeline(
                        ctx.clone(),
                        &mut build_res.main_pipeline,
                        plan.stage_table_info.stage_info.clone(),
                        to_table,
                        files,
                        plan.force,
                        plan.stage_table_info.stage_info.copy_options.purge,
                        plan.write_mode.is_overwrite(),
                    )?
                }
            }
        }

        Ok(build_res)
    }

    /// Build distributed pipeline from source node id.
    #[async_backtrace::framed]
    async fn build_cluster_copy_into_table_pipeline(
        &self,
        distributed_plan: &CopyPlanType,
    ) -> Result<PipelineBuildResult> {
        let (
            catalog_info,
            database_name,
            table_name,
            stage_info,
            files,
            force,
            purge,
            is_overwrite,
        );
        let mut build_res = match distributed_plan {
            CopyPlanType::DistributedCopyIntoTableFromStage(plan) => {
                catalog_info = plan.catalog_info.clone();
                database_name = plan.database_name.clone();
                table_name = plan.table_name.clone();
                stage_info = plan.stage_table_info.stage_info.clone();
                files = plan.files.clone();
                force = plan.force;
                purge = plan.stage_table_info.stage_info.copy_options.purge;
                is_overwrite = plan.write_mode.is_overwrite();
                // add exchange plan node to enable distributed
                // TODO(leiysky): we reuse the id of exchange here,
                // which is not correct. We should generate a new id for insert.
                let exchange_plan = PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(PhysicalPlan::DistributedCopyIntoTableFromStage(Box::new(
                        plan.clone(),
                    ))),
                    kind: FragmentKind::Merge,
                    keys: Vec::new(),
                });

                build_distributed_pipeline(&self.ctx, &exchange_plan, false).await?
            }
            CopyPlanType::CopyIntoTableFromQuery(plan) => {
                catalog_info = plan.catalog_info.clone();
                database_name = plan.database_name.clone();
                table_name = plan.table_name.clone();
                stage_info = plan.stage_table_info.stage_info.clone();
                files = plan.files.clone();
                force = plan.force;
                purge = plan.stage_table_info.stage_info.copy_options.purge;
                is_overwrite = plan.write_mode.is_overwrite();
                // add exchange plan node to enable distributed
                // TODO(leiysky): we reuse the id of exchange here,
                // which is not correct. We should generate a new id
                let exchange_plan = PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(PhysicalPlan::CopyIntoTableFromQuery(Box::new(plan.clone()))),
                    kind: FragmentKind::Merge,
                    keys: Vec::new(),
                });
                build_distributed_pipeline(&self.ctx, &exchange_plan, false).await?
            }
            _ => unreachable!(),
        };
        let to_table = self
            .ctx
            .get_table(catalog_info.catalog_name(), &database_name, &table_name)
            .await?;

        // commit.
        build_commit_data_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            stage_info,
            to_table,
            files,
            force,
            purge,
            is_overwrite,
        )?;
        Ok(build_res)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    #[minitrace::trace(name = "copy_interpreter_execute_v2")]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_interpreter_execute_v2");

        let start = Instant::now();

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        match &self.plan {
            CopyPlan::IntoTable(plan) => {
                let mut pipeline = if plan.enable_distributed {
                    let distributed_plan_op = self
                        .try_transform_copy_plan_from_local_to_distributed(plan)
                        .await?;
                    if let Some(distributed_plan) = distributed_plan_op {
                        let build_res = self
                            .build_cluster_copy_into_table_pipeline(&distributed_plan)
                            .await?;

                        Ok(build_res)
                    } else {
                        self.build_local_copy_into_table_pipeline(plan).await
                    }
                } else {
                    self.build_local_copy_into_table_pipeline(plan).await
                }?;

                let compact_target = CompactTargetTableDescription {
                    catalog: plan.catalog_info.name_ident.catalog_name.clone(),
                    database: plan.database_name.clone(),
                    table: plan.table_name.clone(),
                };

                let trace_ctx = CompactHookTraceCtx {
                    start,
                    operation_name: "copy_into".to_owned(),
                };

                hook_compact(
                    self.ctx.clone(),
                    &mut pipeline.main_pipeline,
                    compact_target,
                    trace_ctx,
                )
                .await;

                Ok(pipeline)
            }
            CopyPlan::IntoStage {
                stage, from, path, ..
            } => {
                self.build_local_copy_into_stage_pipeline(stage, path, from)
                    .await
            }
            CopyPlan::NoFileToCopy => Ok(PipelineBuildResult::create()),
        }

        // let table = self
        //    .ctx
        //    .get_table(&plan.catalog, &plan.database, &plan.table)
        //    .await?;

        // let has_cluster_key = !table.cluster_keys(self.ctx.clone()).is_empty();

        // if !pipeline.main_pipeline.is_empty()
        //    && has_cluster_key
        //    && self.ctx.get_settings().get_enable_auto_reclustering()?
        //{
        //    let ctx = self.ctx.clone();
        //    let catalog = self.plan.catalog.clone();
        //    let database = self.plan.database.to_string();
        //    let table = self.plan.table.to_string();
        //    pipeline.main_pipeline.set_on_finished(move |err| {
        //        //metrics_inc_replace_mutation_time_ms(start.elapsed().as_millis() as u64);
        //        if err.is_none() {
        //            info!("execute replace into finished successfully. running table optimization job.");
        //            let compact_target = CompactTargetTableDescription {
        //                catalog,
        //                database,
        //                table,
        //            };
        //            match  GlobalIORuntime::instance().block_on({
        //                compact_table(ctx, compact_target)
        //            }) {
        //                Ok(_) => {
        //                    info!("execute replace into finished successfully. table optimization job finished.");
        //                }
        //                Err(e) => { info!("execute replace into finished successfully. table optimization job failed. {:?}", e)}
        //            }
        //        }
        //        //metrics_inc_replace_execution_time_ms(start.elapsed().as_millis() as u64);
        //        Ok(())
        //    });
        //}
        // Ok(pipeline)
    }
}

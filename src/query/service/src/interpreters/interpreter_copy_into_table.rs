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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Field;
use databend_common_ast::Span;
use databend_common_ast::ast::ColumnMatchMode;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storage::StageFileInfo;
use databend_common_storage::init_stage_operator;
use databend_common_storage::parquet::infer_schema_with_extension;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_parquet::read_metas_in_parallel_for_copy;
use databend_common_storages_stage::StageTable;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use itertools::Itertools;
use log::debug;
use log::info;

use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::physical_plans::CopyIntoTable;
use crate::physical_plans::CopyIntoTableSource;
use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::TableScan;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyIntoTablePlan;
use crate::sql::plans::Plan;
use crate::stream::DataBlockStream;

pub struct CopyIntoTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyIntoTablePlan,
}

impl CopyIntoTableInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoTablePlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyIntoTablePlan) -> Result<Self> {
        Ok(CopyIntoTableInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(
        &self,
        query: &Plan,
    ) -> Result<(SelectInterpreter, Vec<UpdateStreamMetaReq>)> {
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

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;

        let select_interpreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        Ok((select_interpreter, update_stream_meta))
    }

    #[async_backtrace::framed]
    pub async fn build_physical_plan(
        &self,
        mut table_info: TableInfo,
        plan: &CopyIntoTablePlan,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<(
        PhysicalPlan,
        Vec<UpdateStreamMetaReq>,
        Option<TableSchemaRef>,
    )> {
        let mut new_schema = None;
        let mut update_stream_meta_reqs = vec![];
        let (source, project_columns) = if let Some(ref query) = plan.query {
            let query = if plan.enable_distributed {
                query.remove_exchange_for_select()
            } else {
                *query.clone()
            };

            let (query_interpreter, update_stream_meta) = self.build_query(&query).await?;
            update_stream_meta_reqs = update_stream_meta;
            let query_physical_plan = query_interpreter.build_physical_plan().await?;

            let result_columns = query_interpreter.get_result_columns();
            (
                CopyIntoTableSource::Query(query_physical_plan),
                Some(result_columns),
            )
        } else {
            let mut stage_table_info = plan.stage_table_info.clone();
            if plan.enable_schema_evolution {
                new_schema = Self::infer_schema(&mut stage_table_info, self.ctx.clone())
                    .await
                    .map_err(|e| e.with_context("infer_schema"))?;
            }

            let stage_table = StageTable::try_create(stage_table_info)?;

            let data_source_plan = stage_table
                .read_plan(self.ctx.clone(), None, None, false, false)
                .await?;

            let mut name_mapping = BTreeMap::new();
            for (idx, field) in data_source_plan.schema().fields.iter().enumerate() {
                name_mapping.insert(field.name.clone(), idx);
            }

            (
                CopyIntoTableSource::Stage(PhysicalPlan::new(TableScan {
                    scan_id: 0,
                    name_mapping,
                    stat_info: None,
                    table_index: None,
                    internal_column: None,
                    source: Box::new(data_source_plan),
                    meta: PhysicalPlanMeta::new("TableScan"),
                })),
                None,
            )
        };

        let mut required_values_schema = plan.required_values_schema.clone();
        let mut required_source_schema = plan.required_source_schema.clone();
        if let Some(schema) = &new_schema {
            table_info.meta.schema = schema.clone();
            let data_schema: DataSchema = schema.into();
            required_source_schema = Arc::new(data_schema);
            required_values_schema = required_source_schema.clone();
        }

        let mut root = PhysicalPlan::new(CopyIntoTable {
            required_values_schema,
            values_consts: plan.values_consts.clone(),
            required_source_schema,
            stage_table_info: plan.stage_table_info.clone(),
            table_info,
            write_mode: plan.write_mode,
            validation_mode: plan.validation_mode.clone(),
            project_columns,
            source,
            is_transform: plan.is_transform,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("CopyIntoTable"),
        });

        if plan.enable_distributed {
            root = PhysicalPlan::new(Exchange {
                input: root,
                kind: FragmentKind::Merge,
                keys: Vec::new(),
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("Exchange"),
            });
        }

        let mut next_plan_id = 0;
        root.adjust_plan_id(&mut next_plan_id);

        Ok((root, update_stream_meta_reqs, new_schema))
    }

    async fn infer_schema(
        stage_table_info: &mut StageTableInfo,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Option<TableSchemaRef>> {
        #[allow(clippy::single_match)]
        match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Parquet(_) => {
                let settings = ctx.get_settings();
                let max_threads = settings.get_max_threads()? as usize;
                let max_memory_usage = settings.get_max_memory_usage()?;

                let operator = init_stage_operator(&stage_table_info.stage_info)?;
                // User set the files.
                let files = stage_table_info.files_to_copy.as_ref().expect(
                    "ParquetTableForCopy::do_read_partitions must be called with files_to_copy set",
                );
                let file_infos = files
                    .iter()
                    .filter(|f| f.size > 0)
                    .map(|f| (f.path.clone(), f.size))
                    .collect::<Vec<_>>();
                ctx.set_status_info("[TABLE-SCAN] Infer Parquet Schemas");
                let metas = read_metas_in_parallel_for_copy(
                    &operator,
                    &file_infos,
                    max_threads,
                    max_memory_usage,
                )
                .await?;

                let case_sensitive = stage_table_info.copy_into_table_options.column_match_mode
                    == Some(ColumnMatchMode::CaseSensitive);

                let mut new_schema = stage_table_info.schema.as_ref().to_owned();
                let old_fields: HashMap<String, TableDataType> = stage_table_info
                    .schema
                    .fields
                    .iter()
                    .map(|f| {
                        (
                            if case_sensitive {
                                f.name.clone()
                            } else {
                                f.name.to_lowercase()
                            },
                            f.data_type.clone(),
                        )
                    })
                    .collect::<_>();
                let mut new_fields: HashMap<String, Field> = HashMap::new();
                for meta in &metas {
                    let arrow_schema = infer_schema_with_extension(meta.meta.file_metadata())?;
                    for field in arrow_schema.fields().clone().into_iter() {
                        let name = if case_sensitive {
                            field.name().clone()
                        } else {
                            field.name().to_lowercase()
                        };
                        if !old_fields.contains_key(&name) {
                            if let Some(f) = new_fields.get_mut(&name) {
                                if f.data_type() != field.data_type() {
                                    return Err(ErrorCode::BadBytes(format!(
                                        "data type of {name} mismatch: {} and {}",
                                        f.data_type(),
                                        field.data_type()
                                    )));
                                }
                            } else {
                                new_fields.insert(name, field.as_ref().clone());
                            }
                        }
                    }
                }

                stage_table_info.parquet_metas = Some(metas);
                if new_fields.is_empty() {
                    return Ok(None);
                } else {
                    let new_fields: Vec<_> = new_fields.into_iter().sorted().collect();
                    for (_, f) in new_fields {
                        let mut tf: TableField = (&f).try_into()?;
                        tf.data_type = tf.data_type.wrap_nullable();
                        if let Some(exprs) = &mut stage_table_info.default_exprs {
                            exprs.push(RemoteDefaultExpr::RemoteExpr(RemoteExpr::Constant {
                                scalar: Scalar::Null,
                                data_type: DataType::Null,
                                span: Span::default(),
                            }))
                        }
                        new_schema.add_column(&tf, new_schema.num_fields())?;
                    }
                    let schema = Arc::new(new_schema);
                    stage_table_info.schema = schema.clone();
                    return Ok(Some(schema));
                }
            }
            _ => {}
        }
        Ok(None)
    }

    fn get_copy_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let return_all = !self
            .plan
            .stage_table_info
            .copy_into_table_options
            .return_failed_only;
        let cs = self.ctx.get_copy_status();

        let mut results = cs.files.iter().collect::<Vec<_>>();
        results.sort_by(|a, b| a.key().cmp(b.key()));

        let n = cs.files.len();
        let mut files = Vec::with_capacity(n);
        let mut rows_loaded = Vec::with_capacity(n);
        let mut errors_seen = Vec::with_capacity(n);
        let mut first_error = Vec::with_capacity(n);
        let mut first_error_line = Vec::with_capacity(n);

        for entry in results {
            let status = entry.value();
            if let Some(err) = &status.error {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(err.num_errors as i32);
                first_error.push(Some(err.first_error.error.to_string().clone()));
                first_error_line.push(Some(err.first_error.line as i32 + 1));
            } else if return_all {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(0);
                first_error.push(None);
                first_error_line.push(None);
            }
        }
        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(files),
            Int32Type::from_data(rows_loaded),
            Int32Type::from_data(errors_seen),
            StringType::from_opt_data(first_error),
            Int32Type::from_opt_data(first_error_line),
        ])];
        Ok(blocks)
    }

    /// Build commit insertion pipeline.
    async fn commit_insertion(
        &self,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        files_to_copy: Vec<StageFileInfo>,
        duplicated_files_detected: Vec<String>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        path_prefix: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
        new_schema: Option<TableSchemaRef>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let mut to_table = ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        let mut prev_snapshot_id = None;

        // Commit.
        {
            let mut table_info = to_table.get_table_info().clone();
            if let Some(new_schema) = new_schema {
                let fuse_table = FuseTable::try_from_table(to_table.as_ref())?;
                let base_snapshot = fuse_table.read_table_snapshot().await?;
                prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);

                table_info.meta.fill_field_comments();
                while table_info.meta.field_comments.len() < new_schema.fields.len() {
                    table_info.meta.field_comments.push("".to_string());
                }
                table_info.meta.schema = new_schema;
                to_table = FuseTable::create_and_refresh_table_info(
                    table_info,
                    ctx.get_settings().get_s3_storage_class()?,
                )?
                .into();
            }

            let copied_files_meta_req = PipelineBuilder::build_upsert_copied_files_to_meta_req(
                ctx.clone(),
                to_table.as_ref(),
                &files_to_copy,
                &plan.stage_table_info.copy_into_table_options,
                path_prefix,
            )?;

            to_table.commit_insertion(
                ctx.clone(),
                main_pipeline,
                copied_files_meta_req,
                update_stream_meta,
                plan.write_mode.is_overwrite(),
                prev_snapshot_id,
                deduplicated_label,
                table_meta_timestamps,
            )?;
        }

        // Purge files.
        {
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .into_iter()
                .map(|v| v.path)
                .chain(duplicated_files_detected)
                .collect::<Vec<_>>();
            // set on_finished callback.
            PipelineBuilder::set_purge_files_on_finished(
                ctx.clone(),
                files_to_be_deleted,
                &plan.stage_table_info.copy_into_table_options,
                plan.stage_table_info.stage_info.clone(),
                main_pipeline,
            )?;
        }
        Ok(())
    }

    async fn on_no_files_to_copy(&self) -> Result<PipelineBuildResult> {
        // currently, there is only one thing that we care about:
        //
        // if `purge_duplicated_files_in_copy` and `purge` are all enabled,
        // and there are duplicated files detected, we should clean them up immediately.

        // it might be better to reuse the PipelineBuilder::set_purge_files_on_finished,
        // unfortunately, hooking the on_finished callback of a "blank" pipeline,
        // e.g. `PipelineBuildResult::create` leads to runtime error (during pipeline execution).

        if self.plan.stage_table_info.copy_into_table_options.purge
            && !self
                .plan
                .stage_table_info
                .duplicated_files_detected
                .is_empty()
            && self
                .ctx
                .get_settings()
                .get_enable_purge_duplicated_files_in_copy()?
        {
            info!(
                "purge_duplicated_files_in_copy enabled, number of duplicated files: {}",
                self.plan.stage_table_info.duplicated_files_detected.len()
            );

            PipelineBuilder::purge_files_immediately(
                self.ctx.clone(),
                self.plan.stage_table_info.duplicated_files_detected.clone(),
                self.plan.stage_table_info.stage_info.clone(),
            )
            .await?;
        }
        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoTableInterpreter {
    fn name(&self) -> &str {
        "CopyIntoTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_table_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let plan = &self.plan;
        let to_table = self
            .ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        to_table.check_mutable()?;

        if self.plan.no_file_to_copy {
            info!("no file to copy");
            return self.on_no_files_to_copy().await;
        }

        let snapshot = FuseTable::try_from_table(to_table.as_ref())?
            .read_table_snapshot()
            .await?;
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(to_table.as_ref(), snapshot)?;

        let (physical_plan, update_stream_meta, new_schema) = self
            .build_physical_plan(
                to_table.get_table_info().clone(),
                &self.plan,
                table_meta_timestamps,
            )
            .await?;

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Build commit insertion pipeline.
        {
            let files_to_copy = self
                .plan
                .stage_table_info
                .files_to_copy
                .clone()
                .unwrap_or_default();

            let duplicated_files_detected =
                self.plan.stage_table_info.duplicated_files_detected.clone();

            self.commit_insertion(
                &mut build_res.main_pipeline,
                &self.plan,
                files_to_copy,
                duplicated_files_detected,
                update_stream_meta,
                unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                self.plan.path_prefix.clone(),
                table_meta_timestamps,
                new_schema,
            )
            .await?;
        }

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog_info.catalog_name().to_string(),
                self.plan.database_name.to_string(),
                self.plan.table_name.to_string(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = if self.plan.no_file_to_copy {
            vec![DataBlock::empty_with_schema(&self.plan.schema())]
        } else {
            self.get_copy_into_table_result()?
        };

        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

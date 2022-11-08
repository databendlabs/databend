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
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_catalog::plan::CopyInfo;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storages_stage::StageFilePartition;
use common_storages_stage::StageFileStatus;
use common_storages_stage::StageTable;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;

pub struct CopyInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

impl CopyInterpreterV2 {
    /// Create a CopyInterpreterV2 with context and [`CopyPlanV2`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlanV2) -> Result<Self> {
        Ok(CopyInterpreterV2 { ctx, plan })
    }

    async fn execute_copy_into_stage(
        &self,
        stage: &UserStageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (s_expr, metadata, bind_context) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => (s_expr, metadata, bind_context),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreterV2::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
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
        let stage_table_info = StageTableInfo {
            schema: data_schema.clone(),
            stage_info: stage.clone(),
            path: path.to_string(),
            files: vec![],
        };

        let mut build_res = select_interpreter.execute2().await?;
        let table = StageTable::try_create(stage_table_info)?;

        append2table(
            self.ctx.clone(),
            table.clone(),
            data_schema.clone(),
            &mut build_res,
            false,
            true,
            AppendMode::Normal,
        )?;
        Ok(build_res)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_copy_into_table(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[String],
        path: &str,
        pattern: &str,
        force: bool,
        stage_table_info: &StageTableInfo,
    ) -> Result<PipelineBuildResult> {
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let copy_info = CopyInfo {
            force,
            files: files.to_vec(),
            path: path.to_string(),
            pattern: pattern.to_string(),
            stage_info: stage_table_info.stage_info.clone(),
            into_table_catalog_name: catalog_name.to_string(),
            into_table_database_name: database_name.to_string(),
            into_table_name: table_name.to_string(),
        };
        let pushdown = PushDownInfo {
            projection: None,
            filters: vec![],
            prewhere: None,
            limit: None,
            order_by: vec![],
            copy: Some(copy_info),
        };
        let ctx = self.ctx.clone();

        // Get the source plan with all files.
        let read_source_plan = stage_table
            .read_plan_with_catalog(ctx.clone(), catalog_name.to_string(), Some(pushdown))
            .await?;

        // Remove the AlreadyCopied files from the source plan.
        let mut need_copy_source_plan = read_source_plan.clone();
        need_copy_source_plan.parts.retain(|v| {
            if let Some(sv) = v.as_any().downcast_ref::<StageFilePartition>() {
                return sv.status == StageFileStatus::NeedCopy;
            }
            false
        });

        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        stage_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());

        let mut copied_file_infos = vec![];
        for part in &need_copy_source_plan.parts {
            if let Some(stage_file_info) = part.as_any().downcast_ref::<StageFilePartition>() {
                copied_file_infos.push(stage_file_info.clone());
            }
        }

        // Build pipeline.
        let mut build_res = PipelineBuildResult::create();
        {
            // InputContext.
            let files = copied_file_infos
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();

            let table_ctx: Arc<dyn TableContext> = ctx.clone();
            let operator = StageTable::get_op(&table_ctx, &stage_table_info.stage_info)?;
            let settings = ctx.get_settings();
            let schema = stage_table_info.schema.clone();
            let stage_info = stage_table_info.stage_info.clone();
            let compact_threshold = stage_table.get_block_compact_thresholds();
            let input_ctx = Arc::new(
                InputContext::try_create_from_copy(
                    operator,
                    settings,
                    schema,
                    stage_info,
                    files,
                    ctx.get_scan_progress(),
                    compact_threshold,
                )
                .await?,
            );
            input_ctx
                .format
                .exec_copy(input_ctx.clone(), &mut build_res.main_pipeline)?;

            // Limit.
            let limit = stage_table_info.stage_info.copy_options.size_limit;
            if limit > 0 {
                build_res.main_pipeline.resize(1)?;
                build_res.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        TransformLimit::try_create(
                            Some(limit),
                            0,
                            transform_input_port,
                            transform_output_port,
                        )
                    },
                )?;
            }

            // Append data.
            to_table.append_data(
                ctx.clone(),
                &mut build_res.main_pipeline,
                AppendMode::Copy,
                false,
            )?;
        }

        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                // capture out variable
                let ctx = ctx.clone();
                let to_table = to_table.clone();

                return GlobalIORuntime::instance().block_on(async move {
                    // Commit
                    let operations = ctx.consume_precommit_blocks();
                    to_table
                        .commit_insertion(ctx.clone(), operations, false)
                        .await?;

                    // TODO(bohu): Purge copied files
                    // TODO(bohu): Upsert copied files
                    Ok(())
                });
            }
            Err(may_error.as_ref().unwrap().clone())
        });

        Ok(build_res)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreterV2 {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "copy_interpreter_execute_v2", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match &self.plan {
            CopyPlanV2::IntoTable {
                catalog_name,
                database_name,
                table_name,
                files,
                pattern,
                from,
                force,
                ..
            } => match &from.source_info {
                DataSourceInfo::StageSource(table_info) => {
                    let path = &table_info.path;
                    self.execute_copy_into_table(
                        catalog_name,
                        database_name,
                        table_name,
                        files,
                        path,
                        pattern,
                        *force,
                        table_info,
                    )
                    .await
                }
                other => Err(ErrorCode::Internal(format!(
                    "Cannot list files for the source info: {:?}",
                    other
                ))),
            },
            CopyPlanV2::IntoStage {
                stage, from, path, ..
            } => self.execute_copy_into_stage(stage, path, from).await,
        }
    }
}

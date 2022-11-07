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
use std::collections::BTreeMap;
use std::sync::Arc;

use common_catalog::plan::CopyInfo;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::UserStageInfo;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storages_stage::StageFilePartition;
use common_storages_stage::StageTable;

use crate::catalogs::Catalog;
use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;

const MAX_QUERY_COPIED_FILES_NUM: usize = 50;
const TABLE_COPIED_FILE_KEY_EXPIRE_AFTER_DAYS: Option<u64> = Some(7);
pub struct CopyInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

impl CopyInterpreterV2 {
    /// Create a CopyInterpreterV2 with context and [`CopyPlanV2`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlanV2) -> Result<Self> {
        Ok(CopyInterpreterV2 { ctx, plan })
    }

    async fn do_upsert_copied_files_info(
        expire_at: Option<u64>,
        tenant: String,
        database_name: String,
        table_id: u64,
        copy_stage_files: &mut BTreeMap<String, TableCopiedFileInfo>,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        let req = UpsertTableCopiedFileReq {
            table_id,
            file_info: copy_stage_files.clone(),
            expire_at,
        };
        catalog
            .upsert_table_copied_file_info(&tenant, &database_name, req)
            .await?;
        copy_stage_files.clear();
        Ok(())
    }

    async fn upsert_copied_files_info(
        tenant: String,
        database_name: String,
        table_id: u64,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        tracing::debug!("upsert_copied_files_info: {:?}", copy_stage_files);

        if copy_stage_files.is_empty() {
            return Ok(());
        }

        let expire_at = TABLE_COPIED_FILE_KEY_EXPIRE_AFTER_DAYS
            .map(|after_days| after_days * 86400 + Utc::now().timestamp() as u64);
        let mut do_copy_stage_files = BTreeMap::new();
        for (file_name, file_info) in copy_stage_files {
            do_copy_stage_files.insert(file_name.clone(), file_info);
            if do_copy_stage_files.len() > MAX_QUERY_COPIED_FILES_NUM {
                CopyInterpreterV2::do_upsert_copied_files_info(
                    expire_at,
                    tenant.clone(),
                    database_name.clone(),
                    table_id,
                    &mut do_copy_stage_files,
                    catalog.clone(),
                )
                .await?;
            }
        }
        if !do_copy_stage_files.is_empty() {
            CopyInterpreterV2::do_upsert_copied_files_info(
                expire_at,
                tenant.clone(),
                database_name.clone(),
                table_id,
                &mut do_copy_stage_files,
                catalog.clone(),
            )
            .await?;
        }

        Ok(())
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

    async fn execute_copy_into_table(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &Vec<String>,
        path: &str,
        pattern: &str,
        force: bool,
        stage_table_info: &StageTableInfo,
    ) -> Result<PipelineBuildResult> {
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let copy_info = CopyInfo {
            force,
            files: files.clone(),
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
        let read_source_plan = stage_table
            .read_plan_with_catalog(ctx.clone(), catalog_name.to_string(), Some(pushdown))
            .await?;
        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        stage_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());

        let mut build_res = PipelineBuildResult::create();
        stage_table.read_data(ctx.clone(), &read_source_plan, &mut build_res.main_pipeline)?;

        to_table.append_data(
            ctx.clone(),
            &mut build_res.main_pipeline,
            AppendMode::Copy,
            false,
        )?;

        // TODO(bohu): add commit to meta
        let mut stage_file_infos = vec![];
        for part in &read_source_plan.parts {
            if let Some(stage_file_info) = part.as_any().downcast_ref::<StageFilePartition>() {
                stage_file_infos.push(stage_file_info.clone());
            }
        }

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

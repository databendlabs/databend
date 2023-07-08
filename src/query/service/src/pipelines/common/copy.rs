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
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_sql::executor::DistributedCopyIntoTable;
use common_sql::plans::CopyIntoTableMode;
use common_sql::plans::CopyIntoTablePlan;
use common_storage::StageFileInfo;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::pipelines::common::append2table;
use crate::pipelines::common::append2table_without_commit;
use crate::pipelines::common::try_purge_files;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::TransformCastSchema;
use crate::sessions::QueryContext;

pub enum CopyPlanParam {
    CopyIntoTablePlanOption(CopyIntoTablePlan),
    DistributedCopyIntoTable(DistributedCopyIntoTable),
}

#[allow(clippy::too_many_arguments)]
pub fn copy_append_data_and_set_finish(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    source_schema: Arc<DataSchema>,
    plan_option: CopyPlanParam,
    to_table: Arc<dyn Table>,
    files: Vec<StageFileInfo>,
    start: Instant,
    use_commit: bool,
) -> Result<()> {
    let plan_required_source_schema: DataSchemaRef;
    let plan_required_values_schema: DataSchemaRef;
    let plan_values_consts: Vec<Scalar>;
    let plan_stage_table_info: StageTableInfo;
    let plan_force: bool;
    let plan_write_mode: CopyIntoTableMode;
    let local_id;
    match plan_option {
        CopyPlanParam::CopyIntoTablePlanOption(plan) => {
            plan_required_source_schema = plan.required_source_schema;
            plan_required_values_schema = plan.required_values_schema;
            plan_values_consts = plan.values_consts;
            plan_stage_table_info = plan.stage_table_info;
            plan_force = plan.force;
            plan_write_mode = plan.write_mode;
            local_id = ctx.get_cluster().local_id.clone();
        }
        CopyPlanParam::DistributedCopyIntoTable(plan) => {
            plan_required_source_schema = plan.required_source_schema;
            plan_required_values_schema = plan.required_values_schema;
            plan_values_consts = plan.values_consts;
            plan_stage_table_info = plan.stage_table_info;
            plan_force = plan.force;
            plan_write_mode = plan.write_mode;
            local_id = plan.local_node_id;
        }
    }

    debug!("source schema:{:?}", source_schema);
    debug!("required source schema:{:?}", plan_required_source_schema);
    debug!("required values schema:{:?}", plan_required_values_schema);

    if source_schema != plan_required_source_schema {
        // only parquet need cast
        let func_ctx = ctx.get_function_context()?;
        main_pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCastSchema::try_create(
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                plan_required_source_schema.clone(),
                func_ctx.clone(),
            )
        })?;
    }

    if !plan_values_consts.is_empty() {
        fill_const_columns(
            ctx.clone(),
            main_pipeline,
            source_schema,
            plan_required_values_schema.clone(),
            plan_values_consts,
        )?;
    }

    let stage_info_clone = plan_stage_table_info.stage_info;
    let write_mode = plan_write_mode;
    let mut purge = true;
    match write_mode {
        CopyIntoTableMode::Insert { overwrite } => {
            if use_commit {
                append2table(
                    ctx.clone(),
                    to_table,
                    plan_required_values_schema,
                    main_pipeline,
                    None,
                    overwrite,
                    AppendMode::Copy,
                )?;
            } else {
                append2table_without_commit(
                    ctx.clone(),
                    to_table,
                    plan_required_values_schema,
                    main_pipeline,
                    AppendMode::Copy,
                )?
            }
        }
        CopyIntoTableMode::Copy => {
            if !stage_info_clone.copy_options.purge {
                purge = false;
            }

            if use_commit {
                let copied_files = build_upsert_copied_files_to_meta_req(
                    ctx.clone(),
                    to_table.clone(),
                    stage_info_clone.clone(),
                    files.clone(),
                    plan_force,
                )?;
                append2table(
                    ctx.clone(),
                    to_table,
                    plan_required_values_schema,
                    main_pipeline,
                    copied_files,
                    false,
                    AppendMode::Copy,
                )?;
            } else {
                append2table_without_commit(
                    ctx.clone(),
                    to_table,
                    plan_required_values_schema,
                    main_pipeline,
                    AppendMode::Copy,
                )?
            }
        }
        CopyIntoTableMode::Replace => {}
    }

    if local_id == ctx.get_cluster().local_id {
        main_pipeline.set_on_finished(move |may_error| {
            match may_error {
                None => {
                    GlobalIORuntime::instance().block_on(async move {
                        {
                            let status =
                                format!("end of commit, number of copied files:{}", files.len());
                            ctx.set_status_info(&status);
                            info!(status);
                        }

                        // 1. log on_error mode errors.
                        // todo(ariesdevil): persist errors with query_id
                        if let Some(error_map) = ctx.get_maximum_error_per_file() {
                            for (file_name, e) in error_map {
                                error!(
                                    "copy(on_error={}): file {} encounter error {},",
                                    stage_info_clone.copy_options.on_error,
                                    file_name,
                                    e.to_string()
                                );
                            }
                        }

                        // 2. Try to purge copied files if purge option is true, if error will skip.
                        // If a file is already copied(status with AlreadyCopied) we will try to purge them.
                        if purge {
                            try_purge_files(ctx.clone(), &stage_info_clone, &files).await;
                        }

                        // Status.
                        {
                            info!("all copy finished, elapsed:{}", start.elapsed().as_secs());
                        }

                        Ok(())
                    })?;
                }
                Some(error) => {
                    error!(
                        "copy failed, elapsed:{}, reason: {}",
                        start.elapsed().as_secs(),
                        error
                    );
                }
            }
            Ok(())
        });
    } else {
        // remote node does nothing.
        main_pipeline.set_on_finished(move |_| Ok(()))
    }

    Ok(())
}

pub fn build_upsert_copied_files_to_meta_req(
    ctx: Arc<QueryContext>,
    to_table: Arc<dyn Table>,
    stage_info: StageInfo,
    copied_files: Vec<StageFileInfo>,
    force: bool,
) -> Result<Option<UpsertTableCopiedFileReq>> {
    let mut copied_file_tree = BTreeMap::new();
    for file in &copied_files {
        // Short the etag to 7 bytes for less space in metasrv.
        let short_etag = file.etag.clone().map(|mut v| {
            v.truncate(7);
            v
        });
        copied_file_tree.insert(file.path.clone(), TableCopiedFileInfo {
            etag: short_etag,
            content_length: file.size,
            last_modified: Some(file.last_modified),
        });
    }

    let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;

    let upsert_copied_files_request = {
        if stage_info.copy_options.purge && force {
            // if `purge-after-copy` is enabled, and in `force` copy mode,
            // we do not need to upsert copied files into meta server
            info!(
                "[purge] and [force] are both enabled,  will not update copied-files set. ({})",
                &to_table.get_table_info().desc
            );
            None
        } else if copied_file_tree.is_empty() {
            None
        } else {
            debug!("upsert_copied_files_info: {:?}", copied_file_tree);
            let expire_at = expire_hours * 60 * 60 + Utc::now().timestamp() as u64;
            let req = UpsertTableCopiedFileReq {
                file_info: copied_file_tree,
                expire_at: Some(expire_at),
                fail_if_duplicated: !force,
            };
            Some(req)
        }
    };

    Ok(upsert_copied_files_request)
}

pub fn fill_const_columns(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    const_values: Vec<Scalar>,
) -> Result<()> {
    pipeline.add_transform(|transform_input_port, transform_output_port| {
        TransformAddConstColumns::try_create(
            ctx.clone(),
            transform_input_port,
            transform_output_port,
            input_schema.clone(),
            output_schema.clone(),
            const_values.clone(),
        )
    })?;
    Ok(())
}

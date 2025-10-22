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
use std::time::Duration;

use databend_common_ast::ast::CopyIntoTableOptions;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::ParquetFileFormatParams;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::columns::TransformAddConstColumns;
use databend_common_pipeline_transforms::columns::TransformCastSchema;
use databend_common_pipeline_transforms::columns::TransformNullIf;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::plans::CopyIntoTableMode;
use databend_common_storage::StageFileInfo;
use log::debug;
use log::info;

use crate::physical_plans::CopyIntoTable;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

/// This file implements copy into table pipeline builder.
impl PipelineBuilder {
    fn need_null_if_processor<'a>(
        plan: &'a CopyIntoTable,
        source_schema: &Arc<DataSchema>,
        dest_schema: &Arc<DataSchema>,
    ) -> Option<&'a [String]> {
        if plan.is_transform {
            return None;
        }
        if let FileFormatParams::Parquet(ParquetFileFormatParams { null_if, .. }) =
            &plan.stage_table_info.stage_info.file_format_params
        {
            if !null_if.is_empty()
                && source_schema
                    .fields
                    .iter()
                    .zip(dest_schema.fields.iter())
                    .any(|(src_field, dest_field)| {
                        TransformNullIf::column_need_transform(
                            src_field.data_type(),
                            dest_field.data_type(),
                        )
                    })
            {
                return Some(null_if);
            }
        }
        None
    }

    pub fn build_copy_into_table_append(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTable,
        source_schema: Arc<DataSchema>,
        to_table: Arc<dyn Table>,
    ) -> Result<()> {
        let plan_required_source_schema = &plan.required_source_schema;
        let plan_values_consts = &plan.values_consts;
        let plan_required_values_schema = &plan.required_values_schema;
        let plan_write_mode = &plan.write_mode;

        let source_schema = if let Some(null_if) =
            Self::need_null_if_processor(plan, &source_schema, plan_required_source_schema)
        {
            let func_ctx = Arc::new(ctx.get_function_context()?);
            main_pipeline.try_add_transformer(|| {
                TransformNullIf::try_new(
                    source_schema.clone(),
                    plan_required_source_schema.clone(),
                    func_ctx.clone(),
                    null_if,
                )
            })?;
            TransformNullIf::new_schema(&source_schema)
        } else {
            source_schema
        };

        if &source_schema != plan_required_source_schema {
            // only parquet need cast
            let func_ctx = ctx.get_function_context()?;
            main_pipeline.try_add_transformer(|| {
                TransformCastSchema::try_new(
                    source_schema.clone(),
                    plan_required_source_schema.clone(),
                    func_ctx.clone(),
                )
            })?;
        }

        if !plan_values_consts.is_empty() {
            Self::fill_const_columns(
                ctx.clone(),
                main_pipeline,
                source_schema,
                plan_required_values_schema.clone(),
                plan_values_consts,
            )?;
        }

        // append data without commit.
        match plan_write_mode {
            CopyIntoTableMode::Insert { overwrite: _ } => {
                Self::build_append2table_without_commit_pipeline(
                    ctx,
                    main_pipeline,
                    to_table.clone(),
                    plan_required_values_schema.clone(),
                    plan.table_meta_timestamps,
                )?
            }
            CopyIntoTableMode::Replace => {}
            CopyIntoTableMode::Copy => Self::build_append2table_without_commit_pipeline(
                ctx,
                main_pipeline,
                to_table.clone(),
                plan_required_values_schema.clone(),
                plan.table_meta_timestamps,
            )?,
        }
        Ok(())
    }

    pub(crate) fn build_upsert_copied_files_to_meta_req(
        ctx: Arc<QueryContext>,
        to_table: &dyn Table,
        copied_files: &[StageFileInfo],
        options: &CopyIntoTableOptions,
        path_prefix: Option<String>,
    ) -> Result<Option<UpsertTableCopiedFileReq>> {
        let mut copied_file_tree = BTreeMap::new();
        for file in copied_files {
            // Short the etag to 7 bytes for less space in metasrv.
            let short_etag = file.etag.clone().map(|mut v| {
                v.truncate(7);
                v
            });
            let path = if let Some(p) = &path_prefix {
                format!("{}{}", p, file.path)
            } else {
                file.path.clone()
            };
            copied_file_tree.insert(path, TableCopiedFileInfo {
                etag: short_etag,
                content_length: file.size,
                last_modified: file.last_modified,
            });
        }

        let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;

        let upsert_copied_files_request = {
            if options.purge && options.force {
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
                let req = UpsertTableCopiedFileReq {
                    file_info: copied_file_tree,
                    ttl: Some(Duration::from_hours(expire_hours)),
                    insert_if_not_exists: !options.force,
                };
                Some(req)
            }
        };

        Ok(upsert_copied_files_request)
    }

    fn fill_const_columns(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        const_values: &[Scalar],
    ) -> Result<()> {
        pipeline.try_add_transformer(|| {
            let ctx = ctx.get_function_context()?;
            TransformAddConstColumns::try_new(
                ctx,
                input_schema.clone(),
                output_schema.clone(),
                const_values.to_vec(),
            )
        })
    }
}

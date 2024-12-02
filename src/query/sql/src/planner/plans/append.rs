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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_meta_app::principal::COPY_MAX_FILES_COMMIT_MSG;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_metrics::storage::*;
use databend_common_storage::init_stage_operator;
use log::info;

use super::Operator;
use super::Plan;
use super::RelOp;
use crate::executor::physical_plans::PhysicalAppend;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::optimize_append;
use crate::optimizer::SExpr;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;

#[derive(Clone, PartialEq, Eq)]
pub struct Append {
    // Use table index instead of catalog_name, database_name, table_name here, means once a logic plan is built,
    // the target table is determined, and we won't call get_table() again.
    pub table_index: IndexType,
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,
    pub project_columns: Option<Vec<ColumnBinding>>,
}

#[derive(Clone, Debug)]
pub enum AppendType {
    Insert,
    CopyInto,
}

pub async fn create_append_plan_from_subquery(
    subquery: &Plan,
    catalog_name: String,
    database_name: String,
    table: Arc<dyn Table>,
    target_schema: DataSchemaRef,
    forbid_occ_retry: bool,
    ctx: Arc<dyn TableContext>,
    overwrite: bool,
) -> Result<Plan> {
    let (project_columns, source, metadata) = match subquery {
        Plan::Query {
            bind_context,
            s_expr,
            metadata,
            ..
        } => (
            Some(bind_context.columns.clone()),
            *s_expr.clone(),
            metadata.clone(),
        ),
        _ => unreachable!(),
    };

    let table_index = metadata.write().add_table(
        catalog_name,
        database_name,
        table,
        None,
        false,
        false,
        false,
        false,
    );

    let insert_plan = Append {
        table_index,
        required_values_schema: target_schema.clone(),
        values_consts: vec![],
        required_source_schema: target_schema,
        project_columns,
    };

    let optimized_append = optimize_append(insert_plan, source, metadata.clone(), ctx.as_ref())?;

    Ok(Plan::Append {
        s_expr: Box::new(optimized_append),
        metadata: metadata.clone(),
        stage_table_info: None,
        overwrite,
        forbid_occ_retry,
        append_type: AppendType::Insert,
        target_table_index: table_index,
    })
}

impl Hash for Append {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        todo!()
    }
}

impl Append {
    pub async fn collect_files(
        &self,
        ctx: &dyn TableContext,
        stage_table_info: &mut StageTableInfo,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> Result<()> {
        ctx.set_status_info("begin to list files");
        let start = Instant::now();

        let max_files = stage_table_info.copy_into_table_options.max_files;
        let max_files = if max_files == 0 {
            None
        } else {
            Some(max_files)
        };

        let thread_num = ctx.get_settings().get_max_threads()? as usize;
        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        let options = &stage_table_info.copy_into_table_options;
        let all_source_file_infos = if operator.info().native_capability().blocking {
            if options.force {
                stage_table_info
                    .files_info
                    .blocking_list(&operator, max_files)
            } else {
                stage_table_info.files_info.blocking_list(&operator, None)
            }
        } else if options.force {
            stage_table_info
                .files_info
                .list(&operator, thread_num, max_files)
                .await
        } else {
            stage_table_info
                .files_info
                .list(&operator, thread_num, None)
                .await
        }?;

        let num_all_files = all_source_file_infos.len();

        let end_get_all_source = Instant::now();
        let cost_get_all_files = end_get_all_source.duration_since(start).as_millis();
        metrics_inc_copy_collect_files_get_all_source_files_milliseconds(cost_get_all_files as u64);

        ctx.set_status_info(&format!(
            "end list files: got {} files, time used {:?}",
            num_all_files,
            start.elapsed()
        ));

        let (need_copy_file_infos, duplicated) = if options.force {
            if !options.purge && all_source_file_infos.len() > COPY_MAX_FILES_PER_COMMIT {
                return Err(ErrorCode::Internal(COPY_MAX_FILES_COMMIT_MSG));
            }
            info!(
                "force mode, ignore file filtering {}.{}.{}",
                catalog_name, database_name, table_name
            );
            (all_source_file_infos, vec![])
        } else {
            // Status.
            ctx.set_status_info("begin filtering out copied files");

            let filter_start = Instant::now();
            let FilteredCopyFiles {
                files_to_copy,
                duplicated_files,
            } = ctx
                .filter_out_copied_files(
                    catalog_name,
                    database_name,
                    table_name,
                    &all_source_file_infos,
                    max_files,
                )
                .await?;
            ctx.set_status_info(&format!(
                "end filtering out copied files: {}, time used {:?}",
                num_all_files,
                filter_start.elapsed()
            ));

            let end_filter_out = Instant::now();
            let cost_filter_out = end_filter_out
                .duration_since(end_get_all_source)
                .as_millis();
            metrics_inc_copy_filter_out_copied_files_entire_milliseconds(cost_filter_out as u64);

            (files_to_copy, duplicated_files)
        };

        let num_copied_files = need_copy_file_infos.len();
        let copied_bytes: u64 = need_copy_file_infos.iter().map(|i| i.size).sum();

        info!(
            "collect files with max_files={:?} finished, need to copy {} files, {} bytes; skip {} duplicated files, time used:{:?}",
            max_files,
            need_copy_file_infos.len(),
            copied_bytes,
            num_all_files - num_copied_files,
            start.elapsed()
        );

        stage_table_info.files_to_copy = Some(need_copy_file_infos);
        stage_table_info.duplicated_files_detected = duplicated;

        Ok(())
    }
}

impl Debug for Append {
    fn fmt(&self, _f: &mut Formatter) -> std::fmt::Result {
        // let CopyIntoTablePlan {
        //     catalog_info,
        //     database_name,
        //     table_name,
        //     no_file_to_copy,
        //     validation_mode,
        //     stage_table_info,
        //     query,
        //     ..
        // } = self;
        // write!(
        //     f,
        //     "Copy into {:}.{database_name:}.{table_name:}",
        //     catalog_info.catalog_name()
        // )?;
        // write!(f, ", no_file_to_copy: {no_file_to_copy:?}")?;
        // write!(f, ", validation_mode: {validation_mode:?}")?;
        // write!(f, ", from: {stage_table_info:?}")?;
        // write!(f, " query: {query:?}")?;
        Ok(())
    }
}

impl Append {
    fn copy_into_table_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("File", DataType::String),
            DataField::new("Rows_loaded", DataType::Number(NumberDataType::Int32)),
            DataField::new("Errors_seen", DataType::Number(NumberDataType::Int32)),
            DataField::new(
                "First_error",
                DataType::Nullable(Box::new(DataType::String)),
            ),
            DataField::new(
                "First_error_line",
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
            ),
        ])
    }

    pub fn schema(append_type: &AppendType) -> DataSchemaRef {
        match append_type {
            AppendType::CopyInto => Self::copy_into_table_schema(),
            AppendType::Insert => Arc::new(DataSchema::empty()),
        }
    }

    pub fn target_table(
        metadata: &MetadataRef,
        table_index: IndexType,
    ) -> (Arc<dyn Table>, String, String, String) {
        let metadata = metadata.read();
        let t = metadata.table(table_index);
        (
            t.table(),
            t.catalog().to_string(),
            t.database().to_string(),
            t.name().to_string(),
        )
    }
}

impl Operator for Append {
    fn rel_op(&self) -> RelOp {
        RelOp::Append
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_append(
        &mut self,
        s_expr: &SExpr,
        plan: &crate::plans::Append,
    ) -> Result<PhysicalPlan> {
        if plan
            .project_columns
            .as_ref()
            .is_some_and(|p| p.len() != plan.required_source_schema.num_fields())
        {
            return Err(ErrorCode::BadArguments(format!(
                "Fields in select statement is not equal with expected, select fields: {}, insert fields: {}",
                plan.project_columns.as_ref().unwrap().len(),
                plan.required_source_schema.num_fields(),
            )));
        }
        let target_table = self.metadata.read().table(plan.table_index).table();

        let column_set = plan
            .project_columns
            .as_ref()
            .map(|project_columns| project_columns.iter().map(|c| c.index).collect())
            .unwrap_or_default();
        let source = self.build(s_expr.child(0)?, column_set).await?;

        Ok(PhysicalPlan::Append(Box::new(PhysicalAppend {
            plan_id: 0,
            input: Box::new(source),
            required_values_schema: plan.required_values_schema.clone(),
            values_consts: plan.values_consts.clone(),
            required_source_schema: plan.required_source_schema.clone(),
            table_info: target_table.get_table_info().clone(),
            project_columns: plan.project_columns.clone(),
        })))
    }
}

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
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::COPY_MAX_FILES_COMMIT_MSG;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_metrics::storage::*;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFileInfo;
use log::info;

use super::Operator;
use super::RelOp;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnBinding;

#[derive(Clone, PartialEq, Eq)]
pub struct CopyIntoTablePlan {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,
    pub project_columns: Option<Vec<ColumnBinding>>,
    pub mutation_kind: MutationKind,
}

#[derive(Clone, Debug)]
pub struct StageContext {
    pub purge: bool,
    pub force: bool,
    pub files_to_copy: Vec<StageFileInfo>,
    pub duplicated_files_detected: Vec<String>,
    pub stage_info: StageInfo,
}

impl Hash for CopyIntoTablePlan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.catalog_name.hash(state);
        self.database_name.hash(state);
        self.table_name.hash(state);
    }
}

impl CopyIntoTablePlan {
    pub async fn collect_files(
        &self,
        ctx: &dyn TableContext,
        stage_table_info: &mut StageTableInfo,
        force: bool,
    ) -> Result<()> {
        ctx.set_status_info("begin to list files");
        let start = Instant::now();

        let max_files = stage_table_info.stage_info.copy_options.max_files;
        let stage_table_info = &self.stage_table_info;
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
                "force mode, ignore file filtering. ({}.{})",
                &self.database_name, &self.table_name
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
                    &self.catalog_name,
                    &self.database_name,
                    &self.table_name,
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

impl Debug for CopyIntoTablePlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            no_file_to_copy,
            validation_mode,
            stage_table_info,
            query,
            ..
        } = self;
        write!(
            f,
            "Copy into {:}.{database_name:}.{table_name:}",
            catalog_info.catalog_name()
        )?;
        write!(f, ", no_file_to_copy: {no_file_to_copy:?}")?;
        write!(f, ", validation_mode: {validation_mode:?}")?;
        write!(f, ", from: {stage_table_info:?}")?;
        write!(f, " query: {query:?}")?;
        Ok(())
    }
}

impl CopyIntoTablePlan {
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

    pub fn schema(&self) -> DataSchemaRef {
        match self.mutation_kind {
            MutationKind::CopyInto => Self::copy_into_table_schema(),
            MutationKind::Insert => Arc::new(DataSchema::empty()),
            _ => unreachable!(),
        }
    }
}

impl Operator for CopyIntoTablePlan {
    fn rel_op(&self) -> RelOp {
        RelOp::CopyIntoTable
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_copy_into_table(
        &mut self,
        s_expr: &SExpr,
        plan: &crate::plans::CopyIntoTablePlan,
    ) -> Result<PhysicalPlan> {
        let to_table = self
            .ctx
            .get_table(&plan.catalog_name, &plan.database_name, &plan.table_name)
            .await?;

        let source = self.build(s_expr.child(0)?, Default::default()).await?;

        Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
            plan_id: 0,
            input: Box::new(source),
            required_values_schema: plan.required_values_schema.clone(),
            values_consts: plan.values_consts.clone(),
            required_source_schema: plan.required_source_schema.clone(),
            table_info: to_table.get_table_info().clone(),
            project_columns: None,
        })))
    }
}

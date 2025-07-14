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

use std::any::Any;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::{DataField, DataSchemaRef};
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::plans::CopyIntoTableMode;
use databend_common_sql::plans::ValidationMode;
use databend_common_sql::ColumnBinding;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoTable {
    pub meta: PhysicalPlanMeta,
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,
    pub write_mode: CopyIntoTableMode,
    pub validation_mode: ValidationMode,
    pub stage_table_info: StageTableInfo,
    pub table_info: TableInfo,

    pub project_columns: Option<Vec<ColumnBinding>>,
    pub source: CopyIntoTableSource,
    pub is_transform: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for CopyIntoTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![]))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        match &self.source {
            CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v)),
            CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v)),
        }
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        match &mut self.source {
            CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v)),
            CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v)),
        }
    }

    fn to_format_node(
        &self,
        _ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(
            format!("CopyIntoTable: {}", self.table_info),
            children,
        ))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        match &self.source {
            CopyIntoTableSource::Query(_) => {
                let mut new_copy_into_table = self.clone();
                assert_eq!(children.len(), 1);
                let input = children.pop().unwrap();
                new_copy_into_table.source = CopyIntoTableSource::Query(input);
                Box::new(new_copy_into_table)
            }
            CopyIntoTableSource::Stage(_) => {
                let mut new_copy_into_table = self.clone();
                assert_eq!(children.len(), 1);
                let input = children.pop().unwrap();
                new_copy_into_table.source = CopyIntoTableSource::Stage(input);
                Box::new(new_copy_into_table)
            }
        }
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let to_table = builder.ctx.build_table_by_table_info(&self.table_info, None)?;

        // build_copy_into_table_input
        let source_schema = match &self.source {
            CopyIntoTableSource::Query(input) => {
                input.build_pipeline(builder)?;

                // Reorder the result for select clause
                PipelineBuilder::build_result_projection(
                    &builder.func_ctx,
                    input.output_schema()?,
                    self.project_columns.as_ref().unwrap(),
                    &mut builder.main_pipeline,
                    false,
                )?;
                let fields = self
                    .project_columns
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();

                DataSchemaRefExt::create(fields)
            }
            CopyIntoTableSource::Stage(input) => {
                builder.ctx.set_read_block_thresholds(to_table.get_block_thresholds());

                builder.build_pipeline(input)?;
                self.required_source_schema.clone()
            }
        };

        PipelineBuilder::build_copy_into_table_append(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            self,
            source_schema,
            to_table,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CopyIntoTableSource {
    Query(Box<dyn IPhysicalPlan>),
    Stage(Box<dyn IPhysicalPlan>),
}

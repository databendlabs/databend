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

use chrono::Duration;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::ColumnBinding;
use databend_common_storages_stage::StageSinkTable;
use databend_storages_common_stage::CopyIntoLocationInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoLocation {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub project_columns: Vec<ColumnBinding>,
    pub input_data_schema: DataSchemaRef,
    pub input_table_schema: TableSchemaRef,
    pub info: CopyIntoLocationInfo,
}

#[typetag::serde]
impl IPhysicalPlan for CopyIntoLocation {
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
        Ok(DataSchemaRefExt::create(vec![
            DataField::new("rows_unloaded", DataType::Number(NumberDataType::UInt64)),
            DataField::new("input_bytes", DataType::Number(NumberDataType::UInt64)),
            DataField::new("output_bytes", DataType::Number(NumberDataType::UInt64)),
        ]))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        _ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(
            "CopyIntoLocation".to_string(),
            children,
        ))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        // Reorder the result for select clause
        PipelineBuilder::build_result_projection(
            &builder.func_ctx,
            self.input.output_schema()?,
            &self.project_columns,
            &mut builder.main_pipeline,
            false,
        )?;

        // The stage table that copying into
        let to_table = StageSinkTable::create(self.info.clone(), self.input_table_schema.clone())?;

        // StageSinkTable needs not to hold the table meta timestamps invariants, just pass a dummy one
        let dummy_table_meta_timestamps = TableMetaTimestamps::new(None, Duration::hours(1));
        PipelineBuilder::build_append2table_with_commit_pipeline(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            to_table,
            self.input_data_schema.clone(),
            None,
            vec![],
            false,
            unsafe { builder.settings.get_deduplicate_label()? },
            dummy_table_meta_timestamps,
        )
    }
}

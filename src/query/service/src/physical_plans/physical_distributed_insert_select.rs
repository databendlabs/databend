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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::TableInfoWithBranch;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::TransformCastSchema;
use databend_common_sql::ColumnBinding;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedInsertSelect {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfoWithBranch,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for DistributedInsertSelect {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(DistributedInsertSelect {
            meta: self.meta.clone(),
            input,
            table_info: self.table_info.clone(),
            insert_schema: self.insert_schema.clone(),
            select_schema: self.select_schema.clone(),
            select_column_bindings: self.select_column_bindings.clone(),
            cast_needed: self.cast_needed,
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let select_schema = &self.select_schema;
        let insert_schema = &self.insert_schema;
        // should render result for select
        PipelineBuilder::build_result_projection(
            &builder.func_ctx,
            self.input.output_schema()?,
            &self.select_column_bindings,
            &mut builder.main_pipeline,
            false,
        )?;

        if self.cast_needed {
            builder.main_pipeline.try_add_transformer(|| {
                TransformCastSchema::try_new(
                    select_schema.clone(),
                    insert_schema.clone(),
                    builder.func_ctx.clone(),
                )
            })?;
        }

        let table = builder.ctx.build_table_by_table_info(
            &self.table_info.inner,
            self.table_info.branch.as_deref(),
            None,
        )?;

        let source_schema = insert_schema;
        PipelineBuilder::fill_and_reorder_columns(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            table.clone(),
            source_schema.clone(),
        )?;

        table.append_data(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            self.table_meta_timestamps,
        )?;

        Ok(())
    }
}

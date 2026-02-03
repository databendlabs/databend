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
use databend_common_base::base::Version;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnBinding;
use databend_common_storages_stage::StageSinkTable;
use databend_storages_common_stage::CopyIntoLocationInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoLocation {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub project_columns: Vec<ColumnBinding>,
    pub input_data_schema: DataSchemaRef,
    pub input_table_schema: TableSchemaRef,
    pub info: CopyIntoLocationInfo,
    pub partition_by: Option<RemoteExpr>,
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

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![
            DataField::new("rows_unloaded", DataType::Number(NumberDataType::UInt64)),
            DataField::new("input_bytes", DataType::Number(NumberDataType::UInt64)),
            DataField::new("output_bytes", DataType::Number(NumberDataType::UInt64)),
        ]))
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
        PhysicalPlan::new(CopyIntoLocation {
            input: children.remove(0),
            meta: self.meta.clone(),
            project_columns: self.project_columns.clone(),
            input_data_schema: self.input_data_schema.clone(),
            input_table_schema: self.input_table_schema.clone(),
            info: self.info.clone(),
            partition_by: self.partition_by.clone(),
        })
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
        let to_table = StageSinkTable::create(
            self.info.clone(),
            self.input_table_schema.clone(),
            sink_create_by(&builder.ctx.get_version().semantic),
        )?;

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

fn sink_create_by(version: &Version) -> String {
    const CREATE_BY_LEN: usize = 24; // "Databend 1.2.333-nightly".len();

    // example:  1.2.333-nightly
    // tags may contain other items like `1.2.680-p2`, we will fill it with `1.2.680-p2.....`
    let mut create_by = format!(
        "Databend {}.{}.{}-{:.<7}",
        version.major,
        version.minor,
        version.patch,
        version.pre.as_str()
    );

    if create_by.len() != CREATE_BY_LEN {
        create_by = format!("{:.<24}", create_by);
        create_by.truncate(24);
    }
    create_by
}

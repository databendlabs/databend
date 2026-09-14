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

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_storages_paimon::PaimonTable;
use databend_common_storages_paimon::TransformPaimonWriteRoute;

use crate::physical_plans::format::PaimonWriteRouteFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

pub const PAIMON_ROUTE_KEY_NAME: &str = "__paimon_route_key";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PaimonWriteRoute {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfo,
    pub route_column_offset: usize,
}

impl PaimonWriteRoute {
    pub fn try_create(input: PhysicalPlan, table_info: TableInfo) -> Result<Self> {
        let route_column_offset = input.output_schema()?.num_fields();
        Ok(Self {
            meta: PhysicalPlanMeta::new("PaimonWriteRoute"),
            input,
            table_info,
            route_column_offset,
        })
    }
}

#[typetag::serde]
impl IPhysicalPlan for PaimonWriteRoute {
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
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        fields.push(DataField::new(PAIMON_ROUTE_KEY_NAME, DataType::Binary));
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(PaimonWriteRouteFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.remove(0);
        PhysicalPlan::new(PaimonWriteRoute {
            meta: self.meta.clone(),
            input,
            table_info: self.table_info.clone(),
            route_column_offset: self.route_column_offset,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let paimon_table = PaimonTable::try_from_table(table.as_ref())?;
        let schema = paimon_table.paimon_schema().clone();

        builder
            .main_pipeline
            .try_add_transformer(move || TransformPaimonWriteRoute::try_create(&schema))?;

        Ok(())
    }
}

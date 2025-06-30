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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_stage::CopyIntoLocationInfo;

use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;
use crate::ColumnBinding;

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

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_copy_into_location = self.clone();
                assert_eq!(children.len(), 1);
                new_copy_into_location.input = children[0];
                Box::new(new_copy_into_location)
            }
        }
    }
}

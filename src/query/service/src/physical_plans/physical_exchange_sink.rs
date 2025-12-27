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
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_sql::executor::physical_plans::FragmentKind;

use crate::physical_plans::format::ExchangeSinkFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSink {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    // Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,

    // Fragment ID of sink fragment
    pub destination_fragment_id: usize,

    // Addresses of destination nodes
    pub query_id: String,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
}

impl IPhysicalPlan for ExchangeSink {
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
        Ok(self.schema.clone())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ExchangeSinkFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn is_distributed_plan(&self) -> bool {
        true
    }

    fn display_in_profile(&self) -> bool {
        false
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ExchangeSink {
            meta: self.meta.clone(),
            input,
            schema: self.schema.clone(),
            kind: self.kind.clone(),
            keys: self.keys.clone(),
            destination_fragment_id: self.destination_fragment_id,
            query_id: self.query_id.clone(),
            ignore_exchange: self.ignore_exchange,
            allow_adjust_parallelism: self.allow_adjust_parallelism,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        // ExchangeSink will be appended by `ExchangeManager::execute_pipeline`
        self.input.build_pipeline(builder)
    }
}

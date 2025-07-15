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
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::plans::InsertValue;

use databend_common_exception::Result;
use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::{PipelineBuilder, RawValueSource, ValueSource};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceAsyncSourcer {
    pub meta: PhysicalPlanMeta,
    pub schema: DataSchemaRef,
    pub source: InsertValue,
}

#[typetag::serde]
impl IPhysicalPlan for ReplaceAsyncSourcer {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        builder.main_pipeline.add_source(
            |output| {
                let name_resolution_ctx = NameResolutionContext::try_from(builder.settings.as_ref())?;
                match &self.source {
                    InsertValue::Values { rows } => {
                        let inner = ValueSource::new(rows.clone(), self.schema.clone());
                        AsyncSourcer::create(builder.ctx.clone(), output, inner)
                    }
                    InsertValue::RawValues { data, start } => {
                        let inner = RawValueSource::new(
                            data.clone(),
                            builder.ctx.clone(),
                            name_resolution_ctx,
                            self.schema.clone(),
                            *start,
                        );
                        AsyncSourcer::create(builder.ctx.clone(), output, inner)
                    }
                }
            },
            1,
        )
    }
}

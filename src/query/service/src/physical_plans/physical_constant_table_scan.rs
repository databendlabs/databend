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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::sources::OneBlockSource;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::format::ConstantTableScanFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConstantTableScan {
    pub meta: PhysicalPlanMeta,
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for ConstantTableScan {
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
        Ok(self.output_schema.clone())
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ConstantTableScanFormatter::create(self))
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(ConstantTableScan {
            meta: self.meta.clone(),
            values: self.values.clone(),
            num_rows: self.num_rows,
            output_schema: self.output_schema.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        builder.main_pipeline.add_source(
            |output| {
                let block = if !self.values.is_empty() {
                    DataBlock::new_from_columns(self.values.clone())
                } else {
                    DataBlock::new(vec![], self.num_rows)
                };
                OneBlockSource::create(output, block)
            },
            1,
        )
    }
}

impl ConstantTableScan {
    pub fn name(&self) -> &str {
        if self.num_rows == 0 {
            "EmptyResultScan"
        } else {
            "ConstantTableScan"
        }
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_constant_table_scan(
        &mut self,
        scan: &databend_common_sql::plans::ConstantTableScan,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        debug_assert!(
            scan.schema
                .fields
                .iter()
                .map(|field| field.name().parse::<IndexType>().unwrap())
                .collect::<ColumnSet>()
                .is_superset(&scan.columns)
        );

        let used: ColumnSet = required.intersection(&scan.columns).copied().collect();
        if used.len() < scan.columns.len() {
            let databend_common_sql::plans::ConstantTableScan {
                values,
                num_rows,
                schema,
                ..
            } = scan.prune_columns(used);
            return Ok(PhysicalPlan::new(ConstantTableScan {
                values,
                num_rows,
                output_schema: schema,
                meta: PhysicalPlanMeta::new("ConstantTableScan"),
            }));
        }

        Ok(PhysicalPlan::new(ConstantTableScan {
            values: scan.values.clone(),
            num_rows: scan.num_rows,
            output_schema: scan.schema.clone(),
            meta: PhysicalPlanMeta::new("ConstantTableScan"),
        }))
    }
}

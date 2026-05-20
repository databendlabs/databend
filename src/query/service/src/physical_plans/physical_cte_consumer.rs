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
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::MaterializeCTERefFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::CTESource;

/// This is a leaf operator that consumes the result of a materialized CTE.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializeCTERef {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub cte_name: String,
    pub cte_schema: DataSchemaRef,

    pub meta: PhysicalPlanMeta,
}

#[typetag::serde]
impl IPhysicalPlan for MaterializeCTERef {
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
        Ok(self.cte_schema.clone())
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(MaterializeCTERef {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            cte_name: self.cte_name.clone(),
            cte_schema: self.cte_schema.clone(),
            meta: self.meta.clone(),
        })
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MaterializeCTERefFormatter::create(self))
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let receiver = builder.ctx.get_materialized_cte_receiver(&self.cte_name);
        builder.main_pipeline.add_source(
            |output_port| {
                CTESource::create(builder.ctx.clone(), output_port.clone(), receiver.clone())
            },
            builder.settings.get_max_threads()? as usize,
        )
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cte_consumer(
        &mut self,
        cte_consumer: &databend_common_sql::plans::MaterializedCTERef,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let def_to_ref = cte_consumer
            .column_mapping
            .iter()
            .map(|(k, v)| (*v, *k))
            .collect::<HashMap<_, _>>();
        let cte_required_columns = self
            .cte_required_columns
            .get(&cte_consumer.cte_name)
            .ok_or_else(|| {
                databend_common_exception::ErrorCode::Internal(format!(
                    "CTE required columns not found for CTE name: {}",
                    cte_consumer.cte_name
                ))
            })?;

        let metadata = self.metadata.read();
        let mut cte_output_columns = Vec::with_capacity(cte_required_columns.len());
        for c in cte_required_columns.iter() {
            let index = *def_to_ref.get(c).ok_or_else(|| {
                // Build detailed error message with column names
                let required_cols: Vec<String> = cte_required_columns
                    .iter()
                    .map(|idx| {
                        let col = metadata.column(*idx);
                        format!("{}({})", col.name(), idx)
                    })
                    .collect();

                let available_mappings: Vec<String> = def_to_ref
                    .iter()
                    .map(|(def_idx, ref_idx)| {
                        let def_col = metadata.column(*def_idx);
                        let ref_col = metadata.column(*ref_idx);
                        format!(
                            "{}({}) -> {}({})",
                            def_col.name(),
                            def_idx,
                            ref_col.name(),
                            ref_idx
                        )
                    })
                    .collect();

                let current_col = metadata.column(*c);
                databend_common_exception::ErrorCode::Internal(format!(
                    "Column mapping not found for column {}({}) in CTE: {}.\nRequired columns: [{}]\nAvailable mappings: [{}]",
                    current_col.name(),
                    c,
                    cte_consumer.cte_name,
                    required_cols.join(", "),
                    available_mappings.join(", ")
                ))
            })?;
            cte_output_columns.push(index);
        }
        let mut fields = Vec::new();

        for index in cte_output_columns.iter() {
            let column = metadata.column(*index);
            let data_type = column.data_type();
            fields.push(DataField::new(&index.to_string(), data_type));
        }
        let cte_schema = DataSchemaRefExt::create(fields);
        Ok(PhysicalPlan::new(MaterializeCTERef {
            plan_id: 0,
            stat_info: Some(stat_info),
            cte_name: cte_consumer.cte_name.clone(),
            cte_schema,
            meta: PhysicalPlanMeta::new("MaterializeCTERef"),
        }))
    }
}

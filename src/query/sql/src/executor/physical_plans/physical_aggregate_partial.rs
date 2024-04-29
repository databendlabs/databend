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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
#[allow(unused_imports)]
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::common::AggregateFunctionDesc;
use crate::executor::PhysicalPlan;
use crate::IndexType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregatePartial {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub enable_experimental_aggregate_hashtable: bool,
    pub group_by_display: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregatePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;

        if self.enable_experimental_aggregate_hashtable {
            let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
            for agg in self.agg_funcs.iter() {
                fields.push(DataField::new(
                    &agg.output_column.to_string(),
                    DataType::Binary,
                ));
            }

            let group_types = self
                .group_by
                .iter()
                .map(|index| {
                    Ok(input_schema
                        .field_with_name(&index.to_string())?
                        .data_type()
                        .clone())
                })
                .collect::<Result<Vec<_>>>()?;

            for (idx, data_type) in self.group_by.iter().zip(group_types.iter()) {
                fields.push(DataField::new(&idx.to_string(), data_type.clone()));
            }
            return Ok(DataSchemaRefExt::create(fields));
        }

        let mut fields =
            Vec::with_capacity(self.agg_funcs.len() + self.group_by.is_empty() as usize);
        for agg in self.agg_funcs.iter() {
            fields.push(DataField::new(
                &agg.output_column.to_string(),
                DataType::Binary,
            ));
        }
        if !self.group_by.is_empty() {
            let method = DataBlock::choose_hash_method_with_types(
                &self
                    .group_by
                    .iter()
                    .map(|index| {
                        Ok(input_schema
                            .field_with_name(&index.to_string())?
                            .data_type()
                            .clone())
                    })
                    .collect::<Result<Vec<_>>>()?,
                false,
            )?;
            fields.push(DataField::new("_group_by_key", method.data_type()));
        }

        Ok(DataSchemaRefExt::create(fields))
    }
}

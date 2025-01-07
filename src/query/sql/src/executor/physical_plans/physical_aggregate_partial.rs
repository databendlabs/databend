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
#[allow(unused_imports)]
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use super::SortDesc;
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

    // Order by keys if keys are subset of group by key, then we can use rank to filter data in previous
    pub rank_limit: Option<(Vec<SortDesc>, usize)>,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregatePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let factory = AggregateFunctionFactory::instance();

        let mut fields = Vec::new();
        for func in self.agg_funcs.iter() {
            let schema = match func.sig.udaf {
                None => factory.get_schema(
                    &func.sig.name,
                    func.sig.params.clone(),
                    func.sig.args.clone(),
                ),
                Some(_) => AggregateFunctionFactory::get_udaf_schema(&func.sig.name),
            }?;

            fields.extend(schema.fields);
        }

        for index in self.group_by.iter() {
            let name = index.to_string();
            let data_type = input_schema.field_with_name(&name)?.data_type().clone();
            fields.push(DataField::new(&name, data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

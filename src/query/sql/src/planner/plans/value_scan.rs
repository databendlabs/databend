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
use databend_common_expression::DataSchemaRef;

use crate::executor::physical_plans::PhysicalValueScan;
use crate::executor::physical_plans::Values;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueScan {
    pub values: Values,
    pub dest_schema: DataSchemaRef,
}

impl std::hash::Hash for ValueScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

impl Operator for ValueScan {
    fn rel_op(&self) -> RelOp {
        RelOp::ValueScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_value_scan(
        &mut self,
        plan: &crate::plans::ValueScan,
    ) -> Result<PhysicalPlan> {
        match &plan.values {
            Values::Values(values) => Ok(PhysicalPlan::ValueScan(Box::new(PhysicalValueScan {
                plan_id: 0,
                values: Values::Values(values.clone()),
                output_schema: plan.dest_schema.clone(),
            }))),
            Values::RawValues { rest_str, start } => {
                Ok(PhysicalPlan::ValueScan(Box::new(PhysicalValueScan {
                    plan_id: 0,
                    values: Values::RawValues {
                        rest_str: rest_str.clone(),
                        start: *start,
                    },
                    output_schema: plan.dest_schema.clone(),
                })))
            }
        }
    }
}

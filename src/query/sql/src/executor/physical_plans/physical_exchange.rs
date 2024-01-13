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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::physical_plans::common::FragmentKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnSet;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
}

impl Exchange {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_exchange(
        &mut self,
        s_expr: &SExpr,
        exchange: &crate::plans::Exchange,
        mut required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        if let crate::plans::Exchange::Hash(exprs) = exchange {
            for expr in exprs {
                required.extend(expr.used_columns());
            }
        }

        // 2. Build physical plan.
        let input = Box::new(self.build(s_expr.child(0)?, required).await?);
        let input_schema = input.output_schema()?;
        let mut keys = vec![];
        let mut allow_adjust_parallelism = true;
        let kind = match exchange {
            crate::plans::Exchange::Hash(scalars) => {
                for scalar in scalars {
                    let expr = scalar
                        .type_check(input_schema.as_ref())?
                        .project_column_ref(|index| {
                            input_schema.index_of(&index.to_string()).unwrap()
                        });
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    keys.push(expr.as_remote_expr());
                }
                FragmentKind::Normal
            }
            crate::plans::Exchange::Broadcast => FragmentKind::Expansive,
            crate::plans::Exchange::Merge => FragmentKind::Merge,
            crate::plans::Exchange::MergeSort => {
                allow_adjust_parallelism = false;
                FragmentKind::Merge
            }
        };
        Ok(PhysicalPlan::Exchange(Exchange {
            plan_id: self.next_plan_id(),
            input,
            kind,
            keys,
            allow_adjust_parallelism,
            ignore_exchange: false,
        }))
    }
}

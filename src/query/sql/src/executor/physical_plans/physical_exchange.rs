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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::executor::physical_plans::common::FragmentKind;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::executor::PhysicalPlanMeta;
use crate::optimizer::ir::SExpr;
use crate::ColumnSet;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
}

#[typetag::serde]
impl IPhysicalPlan for Exchange {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn is_distributed_plan(&self) -> bool {
        true
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_exchange = self.clone();
                assert_eq!(children.len(), 1);
                new_exchange.input = children[0];
                Box::new(new_exchange)
            }
        }
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_exchange(
        &mut self,
        s_expr: &SExpr,
        exchange: &crate::plans::Exchange,
        mut required: ColumnSet,
    ) -> Result<Box<dyn IPhysicalPlan>> {
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
        Ok(Box::new(Exchange {
            input,
            kind,
            keys,
            allow_adjust_parallelism,
            ignore_exchange: false,
            meta: PhysicalPlanMeta::new("Exchange"),
        }))
    }
}

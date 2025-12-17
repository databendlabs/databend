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
use databend_common_expression::ConstantFolder;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::ColumnSet;
use databend_common_sql::TypeCheck;

use crate::physical_plans::format::ExchangeFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
}

impl IPhysicalPlan for Exchange {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ExchangeFormatter::create(self))
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
        PhysicalPlan::new(Exchange {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            kind: self.kind.clone(),
            keys: self.keys.clone(),
            ignore_exchange: self.ignore_exchange,
            allow_adjust_parallelism: self.allow_adjust_parallelism,
        })
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_exchange(
        &mut self,
        s_expr: &SExpr,
        exchange: &databend_common_sql::plans::Exchange,
        mut required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        if let databend_common_sql::plans::Exchange::NodeToNodeHash(exprs) = exchange {
            for expr in exprs {
                required.extend(expr.used_columns());
            }
        }

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;
        let mut keys = vec![];
        let mut allow_adjust_parallelism = true;
        let kind = match exchange {
            databend_common_sql::plans::Exchange::NodeToNodeHash(scalars) => {
                for scalar in scalars {
                    let expr = scalar
                        .type_check(input_schema.as_ref())?
                        .project_column_ref(|index| input_schema.index_of(&index.to_string()))?;
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    keys.push(expr.as_remote_expr());
                }
                FragmentKind::Normal
            }
            databend_common_sql::plans::Exchange::Broadcast => FragmentKind::Expansive,
            databend_common_sql::plans::Exchange::Merge => FragmentKind::Merge,
            databend_common_sql::plans::Exchange::MergeSort => {
                allow_adjust_parallelism = false;
                FragmentKind::Merge
            }
        };
        Ok(PhysicalPlan::new(Exchange {
            input,
            kind,
            keys,
            allow_adjust_parallelism,
            ignore_exchange: false,
            meta: PhysicalPlanMeta::new("Exchange"),
        }))
    }
}

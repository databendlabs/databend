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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::SetType;
use databend_common_ast::ast::SetValues;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::BindContext;
use super::Binder;
use crate::planner::semantic::TypeChecker;
use crate::plans::Plan;
use crate::plans::SetPlan;
use crate::plans::SetScalarsOrQuery;
use crate::plans::UnsetPlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_set(
        &mut self,
        bind_context: &mut BindContext,
        set_type: SetType,
        identities: &[Identifier],
        values: &SetValues,
    ) -> Result<Plan> {
        let mut type_checker = TypeChecker::try_create(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false,
        )?;

        let values = match values {
            SetValues::Expr(exprs) => {
                let mut results = vec![];
                for expr in exprs.iter() {
                    let (scalar, _) = *type_checker.resolve(expr.as_ref())?;
                    let expr = scalar.as_expr()?;
                    let (new_expr, _) = ConstantFolder::fold(
                        &expr,
                        &self.ctx.get_function_context()?,
                        &BUILTIN_FUNCTIONS,
                    );
                    match new_expr {
                        databend_common_expression::Expr::Constant { scalar, .. } => {
                            results.push(scalar);
                        }
                        _ => return Err(ErrorCode::SemanticError("value must be constant value")),
                    }
                }
                SetScalarsOrQuery::VarValue(results)
            }
            SetValues::Query(query) => {
                let p = self.clone().bind(&Statement::Query(query.clone())).await?;
                SetScalarsOrQuery::Query(Box::new(p))
            }
        };

        Ok(Plan::Set(Box::new(SetPlan {
            set_type,
            idents: identities
                .iter()
                .map(|x| x.to_string().to_lowercase())
                .collect(),
            values,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_unset(
        &mut self,
        _bind_context: &BindContext,
        unset_type: SetType,
        identities: &[Identifier],
    ) -> Result<Plan> {
        let vars: Vec<String> = identities
            .iter()
            .map(|var| var.name.to_lowercase())
            .collect();
        Ok(Plan::Unset(Box::new(UnsetPlan { unset_type, vars })))
    }
}

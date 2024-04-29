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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::UnSetSource;
use databend_common_ast::ast::UnSetStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::wrap_cast;
use super::BindContext;
use super::Binder;
use crate::planner::semantic::TypeChecker;
use crate::plans::Plan;
use crate::plans::SettingPlan;
use crate::plans::UnSettingPlan;
use crate::plans::VarValue;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_set_variable(
        &mut self,
        bind_context: &mut BindContext,
        is_global: bool,
        variable: &Identifier,
        value: &Expr,
    ) -> Result<Plan> {
        let mut type_checker = TypeChecker::try_create(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false,
        )?;

        let (scalar, _) = *type_checker.resolve(value).await?;
        let scalar = wrap_cast(&scalar, &DataType::String);
        let expr = scalar.as_expr()?;

        let (new_expr, _) =
            ConstantFolder::fold(&expr, &self.ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
        match new_expr {
            databend_common_expression::Expr::Constant { scalar, .. } => {
                let value = scalar.into_string().unwrap();
                let vars = vec![VarValue {
                    is_global,
                    variable: variable.name.to_lowercase(),
                    value,
                }];
                Ok(Plan::SetVariable(Box::new(SettingPlan { vars })))
            }
            _ => Err(ErrorCode::SemanticError("value must be constant value")),
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_unset_variable(
        &mut self,
        _bind_context: &BindContext,
        stmt: &UnSetStmt,
    ) -> Result<Plan> {
        match &stmt.source {
            UnSetSource::Var { variable } => {
                let vars = vec![variable.name.to_lowercase()];
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
            UnSetSource::Vars { variables } => {
                let vars: Vec<String> = variables
                    .iter()
                    .map(|var| var.name.to_lowercase())
                    .collect();
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
        }
    }
}

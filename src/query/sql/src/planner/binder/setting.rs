// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::UnSetSource;
use common_ast::ast::UnSetStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Literal;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use super::BindContext;
use super::Binder;
use crate::executor::PhysicalScalarBuilder;
use crate::planner::semantic::TypeChecker;
use crate::plans::Plan;
use crate::plans::SettingPlan;
use crate::plans::UnSettingPlan;
use crate::plans::VarValue;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_set_variable(
        &mut self,
        bind_context: &BindContext,
        is_global: bool,
        variable: &Identifier<'a>,
        value: &Expr<'a>,
    ) -> Result<Plan> {
        let mut type_checker = TypeChecker::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let variable = variable.name.clone();

        let box (scalar, _) = type_checker.resolve(value, None).await?;
        let schema = Arc::new(DataSchema::empty());

        let builder = PhysicalScalarBuilder::new(&schema);
        let scalar = builder.build(&scalar)?;
        let expr = scalar.as_expr()?;
        let block = DataBlock::empty();
        let func_ctx = self.ctx.try_get_function_context()?;
        let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
        let val = evaluator
            .run(&expr)
            .map_err(|_| ErrorCode::SemanticError(format!("Failed to run expr: {}", expr)))?;
        let value = Literal::try_from(val.into_scalar().unwrap())?;
        let value = match value {
            Literal::Int8(val) => Ok(val.to_string()),
            Literal::Int16(val) => Ok(val.to_string()),
            Literal::Int32(val) => Ok(val.to_string()),
            Literal::Int64(val) => Ok(val.to_string()),
            Literal::UInt8(val) => Ok(val.to_string()),
            Literal::UInt16(val) => Ok(val.to_string()),
            Literal::UInt32(val) => Ok(val.to_string()),
            Literal::UInt64(val) => Ok(val.to_string()),
            Literal::Float32(val) => Ok(val.to_string()),
            Literal::Float64(val) => Ok(val.to_string()),
            Literal::String(val) => Ok(String::from_utf8(val)?),
            other => Err(ErrorCode::BadDataValueType(format!(
                "Variable {:?} unexpected value: {:?}",
                variable, other
            ))),
        };
        let vars = vec![VarValue {
            is_global,
            variable,
            value: value?,
        }];
        Ok(Plan::SetVariable(Box::new(SettingPlan { vars })))
    }

    pub(in crate::planner::binder) async fn bind_unset_variable(
        &mut self,
        _bind_context: &BindContext,
        stmt: &UnSetStmt<'_>,
    ) -> Result<Plan> {
        match stmt.clone().source {
            UnSetSource::Var { variable } => {
                let variable = variable.name;
                let vars = vec![variable];
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
            UnSetSource::Vars { variables } => {
                let mut vars: Vec<String> = vec![];
                for var in variables {
                    vars.push(var.name.clone());
                }
                Ok(Plan::UnSetVariable(Box::new(UnSettingPlan { vars })))
            }
        }
    }
}

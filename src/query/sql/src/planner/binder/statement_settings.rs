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

use std::collections::HashMap;

use chrono_tz::Tz;
use databend_common_ast::ast::SetType;
use databend_common_ast::ast::SetValues;
use databend_common_ast::ast::Settings;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::cast_scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::BindContext;
use crate::Binder;
use crate::TypeChecker;
use crate::plans::Plan;

impl Binder {
    pub async fn bind_statement_settings(
        &mut self,
        bind_context: &mut BindContext,
        settings: &Option<Settings>,
        inner: &Statement,
    ) -> Result<Plan> {
        if let Statement::StatementWithSettings { .. } = inner {
            return Err(ErrorCode::SyntaxException("Invalid statement"));
        }

        let mut statement_settings: HashMap<String, String> = HashMap::new();
        if let Some(settings) = settings {
            let Settings {
                set_type,
                identifiers,
                values,
            } = settings;
            match set_type {
                SetType::SettingsQuery => {
                    let mut type_checker = TypeChecker::try_create(
                        bind_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &[],
                        false,
                    )?;

                    let variables: Vec<String> = identifiers
                        .iter()
                        .map(|x| x.to_string().to_lowercase())
                        .collect();

                    let scalars = match values {
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
                                    Expr::Constant(Constant { scalar, .. }) => {
                                        let scalar = cast_scalar(
                                            None,
                                            scalar.clone(),
                                            &DataType::String,
                                            &BUILTIN_FUNCTIONS,
                                        )?;
                                        results.push(scalar);
                                    }
                                    _ => {
                                        return Err(ErrorCode::SemanticError(
                                            "value must be constant value",
                                        ));
                                    }
                                }
                            }
                            results
                        }
                        SetValues::Query(_) => {
                            return Err(ErrorCode::SemanticError(
                                "query setting value must be scalar",
                            ));
                        }
                        SetValues::None => {
                            return Err(ErrorCode::SemanticError(
                                "query setting value must be scalar",
                            ));
                        }
                    };

                    for (var, scalar) in variables.iter().zip(scalars.into_iter()) {
                        let value = scalar.into_string().unwrap();
                        if var.to_lowercase().as_str() == "timezone" {
                            let tz = value.trim_matches(|c| c == '\'' || c == '\"');
                            tz.parse::<Tz>().map_err(|_| {
                                ErrorCode::InvalidTimezone(format!("Invalid Timezone: {:?}", value))
                            })?;
                        }

                        statement_settings.entry(var.to_string()).or_insert(value);
                    }
                }
                _ => {
                    return Err(ErrorCode::BadArguments("Only support query level setting"));
                }
            }
            self.ctx
                .get_shared_settings()
                .set_batch_settings(&statement_settings, true)?;
            self.bind_statement(bind_context, inner).await
        } else {
            self.bind_statement(bind_context, inner).await
        }
    }
}

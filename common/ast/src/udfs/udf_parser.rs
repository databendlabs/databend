// Copyright 2021 Datafuse Labs.
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

use std::collections::HashSet;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::is_builtin_function;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::parser::expr::ExprTraverser;
use crate::parser::expr::ExprVisitor;

#[derive(Default)]
pub struct UDFParser {
    name: String,
    expr_params: HashSet<String>,
}

impl UDFParser {
    pub async fn parse(
        &mut self,
        name: &str,
        parameters: &[String],
        definition: &str,
    ) -> Result<Expr> {
        let expr = self.parse_definition(definition)?;
        self.verify_definition_expr(name, parameters, &expr).await?;
        Ok(expr)
    }

    fn parse_definition(&mut self, definition: &str) -> Result<Expr> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, definition);

        match tokenizer.tokenize() {
            Ok(tokens) => match Parser::new(tokens, dialect).parse_expr() {
                Ok(definition_expr) => Ok(definition_expr),
                Err(parse_error) => Err(ErrorCode::from(parse_error)),
            },
            Err(tokenize_error) => Err(ErrorCode::SyntaxException(format!(
                "Can not tokenize definition: {}, Error: {:?}",
                definition, tokenize_error
            ))),
        }
    }

    async fn verify_definition_expr(
        &mut self,
        name: &str,
        parameters: &[String],
        definition_expr: &Expr,
    ) -> Result<()> {
        let expr_params = &mut self.expr_params;
        expr_params.clear();
        self.name = name.to_string();

        ExprTraverser::accept(definition_expr, self).await?;
        let expr_params = &self.expr_params;
        let parameters = parameters.iter().cloned().collect::<HashSet<_>>();
        let params_not_declared: HashSet<_> = parameters.difference(expr_params).collect();
        let params_not_used: HashSet<_> = expr_params.difference(&parameters).collect();

        if params_not_declared.is_empty() && params_not_used.is_empty() {
            return Ok(());
        }

        return Err(ErrorCode::SyntaxException(format!(
            "{}{}",
            if params_not_declared.is_empty() {
                "".to_string()
            } else {
                format!("Parameters are not declared: {:?}", params_not_declared)
            },
            if params_not_used.is_empty() {
                "".to_string()
            } else {
                format!("Parameters are not used: {:?}", params_not_used)
            },
        )));
    }
}

#[async_trait]
impl ExprVisitor for UDFParser {
    async fn post_visit(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Identifier(Ident { value, .. }) => {
                let expr_params = &mut self.expr_params;
                expr_params.insert(value.to_string());

                Ok(())
            }
            Expr::Function(Function { name, .. }) => {
                let name = name.to_string();
                if !is_builtin_function(&name) && self.name == name {
                    Err(ErrorCode::SyntaxException(format!(
                        "Function is not builtin or defined: {}",
                        name
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

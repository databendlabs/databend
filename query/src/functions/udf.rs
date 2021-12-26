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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

use crate::sql::ExprTraverser;
use crate::sql::ExprVisitor;

#[derive(Default)]
pub struct UDF {
    params: Vec<u8>,
}

impl UDF {
    pub fn parse_definition(&mut self, definition: &str) -> Result<Expr> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, definition);

        match tokenizer.tokenize() {
            Ok(tokens) => match Parser::new(tokens, dialect).parse_expr() {
                Ok(definition_expr) => {
                    self.verify_definition_expr(&definition_expr)?;
                    Ok(definition_expr)
                }
                Err(parse_error) => Err(ErrorCode::from(parse_error)),
            },
            Err(tokenize_error) => Err(ErrorCode::SyntaxException(format!(
                "Can not tokenize definition: {}, Error: {:?}",
                definition, tokenize_error
            ))),
        }
    }

    fn verify_definition_expr(&mut self, definition_expr: &Expr) -> Result<()> {
        let params = &mut self.params;
        params.clear();

        ExprTraverser::accept(definition_expr, self)?;
        let params = &mut self.params;

        if !params.is_empty() {
            params.sort_unstable();

            let mut previous_param = params[0];
            if previous_param != 0 {
                return Err(ErrorCode::IllegalUDFParams(format!(
                    "UDF params must start with @0, but got: @{}",
                    params[0]
                )));
            } else {
                let rest_params = &params[1..];
                for param in rest_params.iter() {
                    if *param != previous_param && *param != previous_param + 1 {
                        return Err(ErrorCode::IllegalUDFParams(
                            "UDF params must start with @0 and increase by 1, such as @0, @1, @2...")
                        );
                    }

                    previous_param = *param;
                }
            }
        }

        Ok(())
    }
}

impl ExprVisitor for UDF {
    fn post_visit(&mut self, expr: &Expr) -> Result<()> {
        if let Expr::Identifier(Ident { value, .. }) = expr {
            static RE: Lazy<Arc<Regex>> =
                Lazy::new(|| Arc::new(Regex::new(r"@(?P<i>\d+)").unwrap()));
            let index = RE.as_ref().replace_all(value, "$i").parse::<u8>().unwrap();
            let params = &mut self.params;
            params.push(index);
        }

        Ok(())
    }
}

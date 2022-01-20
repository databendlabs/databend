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

use async_trait::async_trait;
use common_ast::udfs::*;
use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use pretty_assertions::assert_eq;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::UnaryOperator;

struct TestFetcher;

#[async_trait]
impl UDFFetcher for TestFetcher {
    async fn get_udf_definition(&self, name: &str) -> Result<UDFDefinition> {
        if name == "test_transformer" {
            let mut parser = UDFParser::default();
            let expr = parser
                .parse(name, &["p".to_string()], "not(isnull(p))")
                .await?;

            Ok(UDFDefinition::new(vec!["p".to_string()], expr))
        } else {
            Err(ErrorCode::UnImplement("Unimplement error"))
        }
    }
}

#[tokio::test]
async fn test_udf_transformer() -> Result<()> {
    let fetcher = &TestFetcher {};
    let result = UDFTransformer::transform_function(
        &Function {
            name: ObjectName(vec![Ident {
                value: "test_transformer".to_string(),
                quote_style: None,
            }]),
            params: vec![],
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier(Ident {
                    value: "test".to_string(),
                    quote_style: None,
                }),
            ))],
            over: None,
            distinct: false,
        },
        fetcher,
    )
    .await?;

    assert_eq!(result, Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Nested(Box::new(Expr::Function(Function {
            name: ObjectName(vec![Ident {
                value: "isnull".to_string(),
                quote_style: None,
            }]),
            params: vec![],
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier(Ident {
                    value: "test".to_string(),
                    quote_style: None,
                })
            ))],
            over: None,
            distinct: false,
        }))))
    });

    assert!(UDFTransformer::transform_function(
        &Function {
            name: ObjectName(vec![Ident {
                value: "test_transformer_not_exist".to_string(),
                quote_style: None,
            }]),
            params: vec![],
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier(Ident {
                    value: "test".to_string(),
                    quote_style: None,
                })
            ))],
            over: None,
            distinct: false,
        },
        fetcher
    )
    .await
    .is_err());

    Ok(())
}

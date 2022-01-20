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

use common_ast::udfs::*;
use common_base::tokio;
use common_exception::Result;
use pretty_assertions::assert_eq;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::UnaryOperator;

#[tokio::test]
async fn test_udf_parser() -> Result<()> {
    let mut parser = UDFParser::default();
    let result = parser
        .parse("test", &["p".to_string()], "not(isnull(p))")
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
                    value: "p".to_string(),
                    quote_style: None,
                })
            ))],
            over: None,
            distinct: false,
        }))))
    });

    assert!(parser
        .parse("test", &["p".to_string()], "not(isnull(p, d))")
        .await
        .is_err());
    assert!(parser
        .parse("test", &["d".to_string()], "not(isnull(p))")
        .await
        .is_err());
    assert!(parser
        .parse("test", &["d".to_string()], "not(unknown_udf(p))")
        .await
        .is_err());
    assert!(parser
        .parse("test", &["d".to_string()], "not(recursive(p))")
        .await
        .is_err());
    assert!(parser
        .parse(
            "test",
            &["d".to_string(), "p".to_string()],
            "not(isnull(p, d))"
        )
        .await
        .is_ok());

    Ok(())
}

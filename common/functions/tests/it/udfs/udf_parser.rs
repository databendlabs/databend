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

use common_exception::Result;
use common_functions::udfs::*;
use pretty_assertions::assert_eq;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::UnaryOperator;

#[test]
fn test_udf_parser() -> Result<()> {
    let mut parser = UDFParser::default();
    let result = parser.parse_definition("tenant", "test", &["p".to_string()], "not(isnull(p))");

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Nested(Box::new(Expr::Function(Function {
            name: ObjectName(vec![Ident {
                value: "isnull".to_string(),
                quote_style: None,
            }]),
            params: vec![],
            args: vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
                value: "p".to_string(),
                quote_style: None,
            }))],
            over: None,
            distinct: false,
        }))))
    });

    assert!(parser
        .parse_definition("tenant", "test", &["p".to_string()], "not(isnull(p, d))")
        .is_err());
    assert!(parser
        .parse_definition("tenant", "test", &["d".to_string()], "not(isnull(p))")
        .is_err());
    assert!(parser
        .parse_definition("tenant", "test", &["d".to_string()], "not(unknown_udf(p))")
        .is_err());
    assert!(parser
        .parse_definition("recursive", "test", &["d".to_string()], "not(recursive(p))")
        .is_err());
    assert!(parser
        .parse_definition(
            "tenant",
            "test",
            &["d".to_string(), "p".to_string()],
            "not(isnull(p, d))"
        )
        .is_ok());

    Ok(())
}

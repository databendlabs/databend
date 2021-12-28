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
fn test_udf_transformer() -> Result<()> {
    let tenant = "tenant_1";
    UDFFactory::register(tenant, "test_transformer", "not(isnull(@0))")?;
    let result = UDFTransformer::transform_function(tenant, &Function {
        name: ObjectName(vec![Ident {
            value: "test_transformer".to_string(),
            quote_style: None,
        }]),
        params: vec![],
        args: vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
            value: "test".to_string(),
            quote_style: None,
        }))],
        over: None,
        distinct: false,
    });

    assert!(result.is_some());
    assert_eq!(result.unwrap(), Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Nested(Box::new(Expr::Function(Function {
            name: ObjectName(vec![Ident {
                value: "isnull".to_string(),
                quote_style: None,
            }]),
            params: vec![],
            args: vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
                value: "test".to_string(),
                quote_style: None,
            }))],
            over: None,
            distinct: false,
        }))))
    });

    assert!(UDFTransformer::transform_function(tenant, &Function {
        name: ObjectName(vec![Ident {
            value: "test_transformer_not_exist".to_string(),
            quote_style: None,
        }]),
        params: vec![],
        args: vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
            value: "test".to_string(),
            quote_style: None,
        }))],
        over: None,
        distinct: false,
    })
    .is_none());

    Ok(())
}

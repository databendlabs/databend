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
fn test_udf_factory() -> Result<()> {
    let tenant = "test_udf_factory_tenant";
    let name = "test_udf_factory";
    let register_result = UDFFactory::register(tenant, name, "not(isnotnull(@0))");

    assert!(register_result.is_ok());
    let definition_expr = UDFFactory::get_definition(tenant, name)?;
    assert_eq!(
        definition_expr,
        Some(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Nested(Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "isnotnull".to_string(),
                    quote_style: None,
                }]),
                params: vec![],
                args: vec![FunctionArg::Unnamed(Expr::Identifier(Ident {
                    value: "@0".to_string(),
                    quote_style: None,
                }))],
                over: None,
                distinct: false,
            }))))
        })
    );

    // test register an existing UDF
    assert!(UDFFactory::register(tenant, "test_udf_factory", "not(isnotnull(@0))").is_ok());

    // test register an scalar function
    assert!(UDFFactory::register(tenant, "isnotnull", "not(isnotnull(@0))").is_err());

    // test register an aggregate function
    assert!(UDFFactory::register(tenant, "count", "not(isnotnull(@0))").is_err());

    // test unregister UDF
    let unregister_result = UDFFactory::unregister(tenant, name);
    assert!(unregister_result.is_ok());

    let definition_expr = UDFFactory::get_definition(tenant, name)?;
    assert_eq!(definition_expr, None);

    Ok(())
}

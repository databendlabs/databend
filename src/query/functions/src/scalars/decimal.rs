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

use common_expression::types::*;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("plus", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }

        if !args_type[0].is_decimal() || !args_type[1].is_decimal() {
            return None;
        }

        let lhs_type = args_type[0].as_decimal().unwrap();
        let rhs_type = args_type[1].as_decimal().unwrap();

        let return_type =
            DecimalDataType::binary_result_type(&lhs_type, &rhs_type, false, false, true).ok()?;
    
        let args_type = args_type.to_owned();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "plus".to_string(),
                args_type: args_type.clone(),
                return_type: DataType::Decimal(return_type.clone()),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let _domain = args_domain[0].as_string().unwrap();
                FunctionDomain::Full
            }),
            eval: Box::new(move |args, tx| {
                plus(args, args_type.clone(), tx, &DataType::Decimal(return_type.clone()))
            }),
        }))
    });
}

fn plus(
    args: &[ValueRef<AnyType>],
    args_type: Vec<DataType>,
    tx: &mut EvalContext,
    return_type: &DataType,
) -> Value<AnyType> {
    todo!()
}

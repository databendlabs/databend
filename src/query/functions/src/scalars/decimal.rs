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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::number::Int64Type;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::UInt8Type;
use common_expression::types::string::StringColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::string::StringDomain;
use common_expression::types::NumberColumn;
use common_expression::types::*;
use common_expression::wrap_nullable;
use common_expression::Column;
use common_expression::Domain;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;
use common_exception::Result;

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
        
        let return_type = DecimalDataType::binary_result_type(&lhs_type, &rhs_type, false, false, true).ok()?;
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "plus".to_string(),
                args_type: args_type.to_owned(),
                return_type: DataType::Decimal(return_type),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let domain = args_domain[0].as_string().unwrap();
                FunctionDomain::Full
            }),
            eval: Box::new(plus),
        }))
    });
}

fn plus(ctx: &mut EvalContext, args: &[ValueRef<AnyType>]) -> Result<Value<AnyType>> {
    todo!()     
}
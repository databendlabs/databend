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

use databend_common_base::base::uuid::Uuid;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256As128Type;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactoryHelper;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;

pub fn register_decimal_to_uuid(registry: &mut FunctionRegistry) {
    let factory = FunctionFactoryHelper::create_1_arg_passthrough_nullable(|_, arg| {
        let size = arg.as_decimal()?;

        if !size.can_carried_by_128() {
            return None;
        }

        fn to_uuid(arg: i128, output: &mut StringColumnBuilder, _: &mut EvalContext<'_>) {
            let uuid = Uuid::from_u128(arg as u128);
            let str = uuid.as_simple().to_string();
            output.put_str(str.as_str());
            output.commit_row();
        }

        Some(Function {
            signature: FunctionSignature {
                name: "to_uuid".to_string(),
                args_type: [DataType::Decimal(*size)].into(),
                return_type: DataType::String,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_, _| FunctionDomain::Full),
                eval: Box::new(|args, ctx| {
                    let arg = args[0].clone();
                    let (decimal_type, _) = DecimalDataType::from_value(&arg).unwrap();
                    match decimal_type {
                        DecimalDataType::Decimal128(_) => {
                            let arg = arg.try_downcast::<Decimal128Type>().unwrap();
                            vectorize_with_builder_1_arg::<Decimal128Type, StringType>(to_uuid)(
                                arg, ctx,
                            )
                            .upcast()
                        }
                        DecimalDataType::Decimal256(_) => {
                            let arg = arg.try_downcast::<Decimal256As128Type>().unwrap();
                            vectorize_with_builder_1_arg::<Decimal256As128Type, StringType>(
                                to_uuid,
                            )(arg, ctx)
                            .upcast()
                        }
                    }
                }),
            },
        })
    });

    registry.register_function_factory("to_uuid", factory);
}

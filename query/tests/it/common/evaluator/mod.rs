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

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::DataField;
use common_datavalues::DataValue;
use common_datavalues::Float64Type;
use common_datavalues::Int32Type;
use common_datavalues::Int64Type;
use common_datavalues::PrimitiveColumn;
use common_exception::Result;
use databend_query::common::Evaluator;
use databend_query::sql::plans::BoundColumnRef;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::plans::Scalar;
use databend_query::sql::ColumnBinding;

use crate::tests::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scalar_evaluator() -> Result<()> {
    // -POW(a + b, 2)
    let scalar = Scalar::FunctionCall(FunctionCall {
        func_name: "negate".to_string(),

        arguments: vec![FunctionCall {
            arguments: vec![
                FunctionCall {
                    arguments: vec![
                        BoundColumnRef {
                            column: ColumnBinding {
                                table_name: None,
                                column_name: "".to_string(),
                                index: 0,
                                data_type: Int32Type::new_impl(),
                                visible_in_unqualified_wildcard: false,
                            },
                        }
                        .into(),
                        BoundColumnRef {
                            column: ColumnBinding {
                                table_name: None,
                                column_name: "".to_string(),
                                index: 1,
                                data_type: Int32Type::new_impl(),
                                visible_in_unqualified_wildcard: false,
                            },
                        }
                        .into(),
                    ],
                    func_name: "plus".to_string(),
                    arg_types: vec![Int32Type::new_impl(), Int32Type::new_impl()],
                    return_type: Int32Type::new_impl(),
                }
                .into(),
                ConstantExpr {
                    value: DataValue::Int64(2),
                    data_type: Int64Type::new_impl(),
                }
                .into(),
            ],
            func_name: "pow".to_string(),
            arg_types: vec![Int64Type::new_impl(), Int64Type::new_impl()],
            return_type: Float64Type::new_impl(),
        }
        .into()],
        arg_types: vec![Float64Type::new_impl()],
        return_type: Float64Type::new_impl(),
    });

    // Block layout:
    //
    //  0  1
    // ------
    //  1  1
    //  2  2
    //  3  3
    let mut block = DataBlock::empty();
    block = block.add_column(
        PrimitiveColumn::<i32>::new_from_vec(vec![1, 2, 3]).arc(),
        DataField::new("0", Int32Type::new_impl()),
    )?;
    block = block.add_column(
        PrimitiveColumn::<i32>::new_from_vec(vec![1, 2, 3]).arc(),
        DataField::new("1", Int32Type::new_impl()),
    )?;

    let func_ctx = create_query_context().await?.try_get_function_context()?;
    let eval = Evaluator::eval_scalar::<String>(&scalar)?;
    let result = eval.eval(&func_ctx, &block)?;

    assert_eq!(result.vector().get(0), DataValue::Float64(-4.0));
    assert_eq!(result.vector().get(1), DataValue::Float64(-16.0));
    assert_eq!(result.vector().get(2), DataValue::Float64(-36.0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eval_const() -> Result<()> {
    // POW(1 + 3, 2)
    let scalar = Scalar::FunctionCall(FunctionCall {
        arguments: vec![
            FunctionCall {
                arguments: vec![
                    ConstantExpr {
                        value: DataValue::Int64(1),
                        data_type: Int32Type::new_impl(),
                    }
                    .into(),
                    ConstantExpr {
                        value: DataValue::Int64(3),
                        data_type: Int32Type::new_impl(),
                    }
                    .into(),
                ],
                func_name: "plus".to_string(),
                arg_types: vec![Int32Type::new_impl(), Int32Type::new_impl()],
                return_type: Int32Type::new_impl(),
            }
            .into(),
            ConstantExpr {
                value: DataValue::Int64(2),
                data_type: Int32Type::new_impl(),
            }
            .into(),
        ],
        func_name: "pow".to_string(),
        arg_types: vec![Int32Type::new_impl(), Int32Type::new_impl()],
        return_type: Float64Type::new_impl(),
    });

    let func_ctx = create_query_context().await?.try_get_function_context()?;
    let eval = Evaluator::eval_scalar::<String>(&scalar)?;
    let (result, result_type) = eval.try_eval_const(&func_ctx)?;

    assert_eq!(result, DataValue::Float64(16.0));
    assert_eq!(result_type, Float64Type::new_impl());
    Ok(())
}

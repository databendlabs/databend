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
use common_datavalues::DataValue;
use common_datavalues::Float64Type;
use common_datavalues::Int32Type;
use common_exception::Result;
use databend_query::sessions::TableContext;
use databend_query::sql::evaluator::Evaluator;
use databend_query::sql::plans::ConstantExpr;
use databend_query::sql::plans::FunctionCall;
use databend_query::sql::plans::Scalar;

use crate::tests::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_eval_const() -> Result<()> {
    // POW(1 + 3, 2)
    let scalar = Scalar::FunctionCall(FunctionCall {
        arguments: vec![
            FunctionCall {
                arguments: vec![
                    ConstantExpr {
                        value: DataValue::Int64(1),
                        data_type: Box::new(Int32Type::new_impl()),
                    }
                    .into(),
                    ConstantExpr {
                        value: DataValue::Int64(3),
                        data_type: Box::new(Int32Type::new_impl()),
                    }
                    .into(),
                ],
                func_name: "plus".to_string(),
                arg_types: vec![Int32Type::new_impl(), Int32Type::new_impl()],
                return_type: Box::new(Int32Type::new_impl()),
            }
            .into(),
            ConstantExpr {
                value: DataValue::Int64(2),
                data_type: Box::new(Int32Type::new_impl()),
            }
            .into(),
        ],
        func_name: "pow".to_string(),
        arg_types: vec![Int32Type::new_impl(), Int32Type::new_impl()],
        return_type: Box::new(Float64Type::new_impl()),
    });

    let (_guard, ctx) = create_query_context().await?;
    let func_ctx = ctx.try_get_function_context()?;
    let eval = Evaluator::eval_scalar(&scalar)?;
    let (result, result_type) = eval.try_eval_const(&func_ctx)?;

    assert_eq!(result, DataValue::Float64(16.0));
    assert_eq!(result_type, Float64Type::new_impl());
    Ok(())
}

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
use common_datavalues::Int32Type;
use common_datavalues::Int64Type;
use common_datavalues::PrimitiveColumn;
use common_datavalues::StringType;
use common_exception::Result;
use common_planners::Expression;
use databend_query::common::ExpressionEvaluator;

use crate::tests::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_expression_evaluator() -> Result<()> {
    // A expression which contains Unary, Binary, ScalarFunction, Column, Literal and Cast:
    //
    //  CAST(-POW(a + b, 2) AS STRING)
    let expr = Expression::Cast {
        expr: Box::new(Expression::UnaryExpression {
            op: "negate".to_string(),
            expr: Box::new(Expression::ScalarFunction {
                op: "pow".to_string(),
                args: vec![
                    Expression::BinaryExpression {
                        left: Box::new(Expression::Column("a".to_string())),
                        op: "+".to_string(),
                        right: Box::new(Expression::Column("b".to_string())),
                    },
                    Expression::Literal {
                        value: DataValue::Int64(2),
                        column_name: None,
                        data_type: Int64Type::new_impl(),
                    },
                ],
            }),
        }),
        data_type: StringType::new_impl(),
        pg_style: false,
    };

    // Block layout:
    //
    //  a  b
    // ------
    //  1  1
    //  2  2
    //  3  3
    let mut block = DataBlock::empty();
    block = block.add_column(
        PrimitiveColumn::<i32>::new_from_vec(vec![1, 2, 3]).arc(),
        DataField::new("a", Int32Type::new_impl()),
    )?;
    block = block.add_column(
        PrimitiveColumn::<i32>::new_from_vec(vec![1, 2, 3]).arc(),
        DataField::new("b", Int32Type::new_impl()),
    )?;

    let func_ctx = create_query_context().await?.try_get_function_context()?;
    let result = ExpressionEvaluator::eval(func_ctx, &expr, &block)?;

    assert_eq!(
        result.get(0),
        DataValue::String("-4.0".to_string().into_bytes())
    );
    assert_eq!(
        result.get(1),
        DataValue::String("-16.0".to_string().into_bytes())
    );
    assert_eq!(
        result.get(2),
        DataValue::String("-36.0".to_string().into_bytes())
    );
    Ok(())
}

// Copyright 2026 Datafuse Labs.
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

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::values::Column;
use databend_common_expression::values::Value;

#[test]
fn test_run_lambda_ignores_physical_nullable_wrapper_for_non_nullable_array() {
    let block = DataBlock::empty();
    let func_ctx = FunctionContext::default();
    let fn_registry = FunctionRegistry::empty();
    let evaluator = Evaluator::new(&block, &func_ctx, &fn_registry);

    let inner_ty = DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)));
    let array_ty = DataType::Array(Box::new(inner_ty.clone()));
    let inner_column = NullableColumn::new_column(
        Int64Type::from_data(vec![1i64, 2, 3]),
        Bitmap::new_constant(true, 3),
    );
    let array_column = Column::Array(Box::new(ArrayColumn::new(
        inner_column,
        vec![0_u64, 3].into(),
    )));

    // Arrays with nullable elements may arrive wrapped physically, even when the
    // logical argument type is not nullable.
    let arg = Value::Column(NullableColumn::new_column(
        array_column,
        Bitmap::new_constant(true, 1),
    ));
    let lambda_expr = RemoteExpr::ColumnRef {
        span: None,
        id: 0,
        data_type: inner_ty,
        display_name: "x".to_string(),
    };

    let result = evaluator
        .run_lambda(
            "array_transform",
            vec![arg],
            vec![array_ty.clone()],
            &lambda_expr,
            &array_ty,
        )
        .unwrap();

    let column = result.into_column().unwrap();
    assert_eq!(column.data_type(), array_ty);
    assert!(matches!(
        unsafe { column.index_unchecked(0) },
        ScalarRef::Array(_)
    ));
}

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

use common_arrow::arrow::bitmap::Bitmap;

use crate::select_columns;
use crate::select_scalar_and_column;
use crate::types::nullable::NullableColumn;
use crate::types::AnyType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::NullableType;
use crate::types::NumberDataType;
use crate::Column;
use crate::DataBlock;
use crate::Expr;
use crate::Scalar;
use crate::Value;

#[derive(Clone, Debug)]
pub enum SelectExpr {
    And(Vec<SelectExpr>),
    Or(Vec<SelectExpr>),
    Compare((SelectOp, Vec<Expr>)),
    Others(Expr),
    BooleanColumnRef((usize, DataType)),
    BooleanConstant((Scalar, DataType)),
}

pub fn build_select_expr(expr: &Expr) -> (SelectExpr, bool) {
    match expr {
        Expr::FunctionCall { function, args, .. } => {
            let func_name = function.signature.name.as_str();
            match func_name {
                "and" | "and_filters" => {
                    let mut and_args = vec![];
                    let mut has_or = false;
                    for arg in args {
                        // Recursively flatten the AND expressions.
                        let (select_expr, exists_or) = build_select_expr(arg);
                        has_or |= exists_or;
                        if let SelectExpr::And(select_expr) = select_expr {
                            and_args.extend(select_expr);
                        } else {
                            and_args.push(select_expr);
                        }
                    }
                    (SelectExpr::And(and_args), has_or)
                }
                "or" => {
                    let mut or_args = vec![];
                    for arg in args {
                        // Recursively flatten the OR expressions.
                        let (select_expr, _) = build_select_expr(arg);
                        if let SelectExpr::Or(select_expr) = select_expr {
                            or_args.extend(select_expr);
                        } else {
                            or_args.push(select_expr);
                        }
                    }
                    (SelectExpr::Or(or_args), true)
                }
                "eq" | "noteq" | "gt" | "lt" | "gte" | "lte" => {
                    let select_op = SelectOp::try_from_func_name(&function.signature.name).unwrap();
                    (SelectExpr::Compare((select_op, args.clone())), false)
                }
                "is_true" => build_select_expr(&args[0]),
                _ => (SelectExpr::Others(expr.clone()), false),
            }
        }
        Expr::ColumnRef { id, data_type, .. } if matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean)) => {
            (
                SelectExpr::BooleanColumnRef((*id, data_type.clone())),
                false,
            )
        }
        Expr::Constant {
            scalar, data_type, ..
        } if matches!(data_type, &DataType::Boolean | &DataType::Nullable(box DataType::Boolean)) => {
            (
                SelectExpr::BooleanConstant((scalar.clone(), data_type.clone())),
                false,
            )
        }
        _ => (SelectExpr::Others(expr.clone()), false),
    }
}

// Build a range selection from a selection array.
pub fn build_range_selection(selection: &[u32], count: usize) -> Vec<(u32, u32)> {
    let mut range_selection = Vec::with_capacity(count);
    let mut start = selection[0];
    let mut idx = 1;
    while idx < count {
        if selection[idx] != selection[idx - 1] + 1 {
            range_selection.push((start, selection[idx - 1] + 1));
            start = selection[idx];
        }
        idx += 1;
    }
    range_selection.push((start, selection[count - 1] + 1));
    range_selection
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SelectStrategy {
    True,
    False,
    ALL,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum SelectOp {
    Equal,
    NotEqual,
    // Greater ">"
    Gt,
    // Less "<"
    Lt,
    // Greater or equal ">="
    Gte,
    // Less or equal "<="
    Lte,
}

impl SelectOp {
    pub fn try_from_func_name(name: &str) -> Option<Self> {
        match name {
            "eq" => Some(Self::Equal),
            "noteq" => Some(Self::NotEqual),
            "gt" => Some(Self::Gt),
            "lt" => Some(Self::Lt),
            "gte" => Some(Self::Gte),
            "lte" => Some(Self::Lte),
            _ => None,
        }
    }

    pub fn reverse(&self) -> Self {
        match &self {
            SelectOp::Equal => SelectOp::Equal,
            SelectOp::NotEqual => SelectOp::NotEqual,
            SelectOp::Gt => SelectOp::Lt,
            SelectOp::Lt => SelectOp::Gt,
            SelectOp::Gte => SelectOp::Lte,
            SelectOp::Lte => SelectOp::Gte,
        }
    }
}

#[inline(always)]
fn equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left == right
}

#[inline(always)]
fn not_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left != right
}

#[inline(always)]
fn greater_than<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left > right
}

#[inline(always)]
fn greater_than_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left >= right
}

#[inline(always)]
fn less_than<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left < right
}

#[inline(always)]
fn less_than_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left <= right
}

pub fn selection_op<T>(op: SelectOp) -> fn(T, T) -> bool
where T: std::cmp::PartialOrd {
    match op {
        SelectOp::Equal => equal::<T>,
        SelectOp::NotEqual => not_equal::<T>,
        SelectOp::Gt => greater_than::<T>,
        SelectOp::Gte => greater_than_equal::<T>,
        SelectOp::Lt => less_than::<T>,
        SelectOp::Lte => less_than_equal::<T>,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn select_values(
    op: SelectOp,
    left: Value<AnyType>,
    right: Value<AnyType>,
    left_data_type: DataType,
    right_data_type: DataType,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    match (left, right) {
        (Value::Scalar(left), Value::Scalar(right)) => select_scalar(
            op,
            left,
            right,
            left_data_type,
            right_data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        ),
        (Value::Column(left), Value::Column(right)) => select_columns(
            op,
            left,
            right,
            left_data_type,
            right_data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        ),
        (Value::Scalar(scalar), Value::Column(column)) => select_scalar_and_column(
            op,
            scalar,
            column,
            right_data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        ),
        (Value::Column(column), Value::Scalar(scalar)) => select_scalar_and_column(
            op.reverse(),
            scalar,
            column,
            left_data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn update_selection_by_boolean_value(
    value: Value<AnyType>,
    data_type: &DataType,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    debug_assert!(
        matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean))
    );

    match data_type {
        DataType::Boolean => {
            let value = value.try_downcast::<BooleanType>().unwrap();
            match value {
                Value::Scalar(scalar) => update_selection_by_scalar_adapt(
                    scalar,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                ),
                Value::Column(column) => update_selection_by_column_adapt(
                    column,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                ),
            }
        }
        DataType::Nullable(box DataType::Boolean) => {
            let nullable_value = value.try_downcast::<NullableType<BooleanType>>().unwrap();
            match nullable_value {
                Value::Scalar(None) => update_selection_by_scalar_adapt(
                    false,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                ),
                Value::Scalar(Some(scalar)) => update_selection_by_scalar_adapt(
                    scalar,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                ),
                Value::Column(NullableColumn { column, validity }) => {
                    let bitmap = &column & &validity;
                    update_selection_by_column_adapt(
                        bitmap,
                        true_selection,
                        false_selection,
                        true_idx,
                        false_idx,
                        select_strategy,
                        count,
                    )
                }
            }
        }
        _ => unreachable!("update_selection_by_boolean_value: {:?}", data_type),
    }
}

pub fn update_selection_by_scalar_adapt(
    scalar: bool,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let has_true = !true_selection.is_empty();
    let has_false = false_selection.1;
    if has_true && has_false {
        update_selection_by_scalar::<true, true>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        update_selection_by_scalar::<true, false>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        update_selection_by_scalar::<false, true>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn update_selection_by_column_adapt(
    column: Bitmap,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let has_true = !true_selection.is_empty();
    let has_false = false_selection.1;
    if has_true && has_false {
        update_selection_by_column::<true, true>(
            column,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        update_selection_by_column::<true, false>(
            column,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        update_selection_by_column::<false, true>(
            column,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn update_selection_by_scalar<const TRUE: bool, const FALSE: bool>(
    scalar: bool,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            if scalar {
                if TRUE {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for i in start..end {
                    let idx = *true_selection.get_unchecked(i);
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            if scalar {
                if TRUE {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for i in start..end {
                    let idx = *false_selection.get_unchecked(i);
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::ALL => {
            if scalar {
                if TRUE {
                    for idx in 0u32..count as u32 {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for idx in 0u32..count as u32 {
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        }
    }
    let true_count = true_idx - *true_start_idx;
    let false_count = false_idx - *false_start_idx;
    *true_start_idx = true_idx;
    *false_start_idx = false_idx;
    if TRUE {
        true_count
    } else {
        count - false_count
    }
}

pub fn update_selection_by_column<const TRUE: bool, const FALSE: bool>(
    column: Bitmap,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            for i in start..end {
                let idx = *true_selection.get_unchecked(i);
                if column.get_bit_unchecked(idx as usize) {
                    if TRUE {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            for i in start..end {
                let idx = *false_selection.get_unchecked(i);
                if column.get_bit_unchecked(idx as usize) {
                    if TRUE {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::ALL => unsafe {
            for idx in 0u32..count as u32 {
                if column.get_bit_unchecked(idx as usize) {
                    if TRUE {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
    }
    let true_count = true_idx - *true_start_idx;
    let false_count = false_idx - *false_start_idx;
    *true_start_idx = true_idx;
    *false_start_idx = false_idx;
    if TRUE {
        true_count
    } else {
        count - false_count
    }
}

fn _get_some_test_data_types() -> Vec<DataType> {
    vec![
        // DataType::Null,
        // DataType::EmptyArray,
        // DataType::EmptyMap,
        // DataType::Boolean,
        // DataType::String,
        // DataType::Bitmap,
        // DataType::Variant,
        // DataType::Timestamp,
        // DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        // DataType::Number(NumberDataType::UInt16),
        // DataType::Number(NumberDataType::UInt32),
        // DataType::Number(NumberDataType::UInt64),
        // DataType::Number(NumberDataType::Int8),
        // DataType::Number(NumberDataType::Int16),
        // DataType::Number(NumberDataType::Int32),
        // DataType::Number(NumberDataType::Int64),
        // DataType::Number(NumberDataType::Float32),
        // DataType::Number(NumberDataType::Float64),
        // DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
        //     precision: 10,
        //     scale: 2,
        // })),
        // DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
        //     precision: 35,
        //     scale: 3,
        // })),
        // DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        // DataType::Nullable(Box::new(DataType::String)),
        // DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        // DataType::Map(Box::new(DataType::Tuple(vec![
        //     DataType::Number(NumberDataType::UInt64),
        //     DataType::String,
        // ]))),
    ]
}

fn _rand_block_for_some_types(num_rows: usize) -> DataBlock {
    let types = _get_some_test_data_types();
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types.iter() {
        columns.push(Column::random(data_type, num_rows));
    }

    let block = DataBlock::new_from_columns(columns);
    block.check_valid().unwrap();

    block
}

#[test]
pub fn test_operation() -> common_exception::Result<()> {
    use rand::Rng;

    use crate::types::NumberScalar;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);

    for _ in 0..num_blocks {
        let len = rng.gen_range(2..30);
        let slice_start = rng.gen_range(0..len - 1);
        let slice_end = rng.gen_range(slice_start..len);

        let random_block_1 = _rand_block_for_some_types(len);
        let random_block_2 = _rand_block_for_some_types(len);

        let random_block_1 = random_block_1.slice(slice_start..slice_end);
        let random_block_2 = random_block_2.slice(slice_start..slice_end);

        let len = slice_end - slice_start;

        let mut selection = vec![0u32; len];
        let mut false_selection = vec![0u32; len];
        let mut count = len;

        for (left, right) in random_block_1
            .columns()
            .iter()
            .zip(random_block_2.columns().iter())
        {
            let op = SelectOp::Gt;
            let number_scalar = Scalar::Number(NumberScalar::UInt8(100));
            let _right_scalar = Value::<AnyType>::Scalar(number_scalar);
            let mut true_idx = 0;
            let mut false_idx = 0;
            count = select_values(
                op,
                left.value.clone(),
                // right_scalar,
                right.value.clone(),
                left.data_type.clone(),
                right.data_type.clone(),
                &mut selection,
                (&mut false_selection, false),
                &mut true_idx,
                &mut false_idx,
                SelectStrategy::ALL,
                count,
            );
        }
        println!("count = {:?}, len = {:?}", count, len);

        if count > 0 {
            let count = count + ((count < len) as usize);
            let left = random_block_1.take(&selection[0..count], &mut None)?;
            let right = random_block_2.take(&selection[0..count], &mut None)?;
            println!("left = \n{:?}\nright = \n{:?}\n", left, right);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn select_scalar(
    op: SelectOp,
    left: Scalar,
    right: Scalar,
    _left_data_type: DataType,
    _right_data_type: DataType,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let result = selection_op(op)(left, right);
    update_selection_by_scalar_adapt(
        result,
        true_selection,
        false_selection,
        true_idx,
        false_idx,
        select_strategy,
        count,
    )
    // match data_type {
    //     DataType::Nullable(_) => {
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Number(NumberDataType::UInt8) => {
    //         let left = left.into_number().unwrap().into_u_int8().unwrap();
    //         let right = right.into_number().unwrap().into_u_int8().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Number(NumberDataType::UInt16) => {
    //         let left = left.into_number().unwrap().into_u_int16().unwrap();
    //         let right = right.into_number().unwrap().into_u_int16().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::String => {
    //         let left = left.into_string().unwrap();
    //         let right = right.into_string().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Boolean => {
    //         let left = left.into_boolean().unwrap();
    //         let right = right.into_boolean().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Decimal(DecimalDataType::Decimal128(_)) => {
    //         let (left, _) = left.into_decimal().unwrap().into_decimal128().unwrap();
    //         let (right, _) = right.into_decimal().unwrap().into_decimal128().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Decimal(DecimalDataType::Decimal256(_)) => {
    //         let (left, _) = left.into_decimal().unwrap().into_decimal256().unwrap();
    //         let (right, _) = right.into_decimal().unwrap().into_decimal256().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Array(_) => {
    //         let left = left.into_array().unwrap();
    //         let right = right.into_array().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     DataType::Map(_) => {
    //         let left = left.into_map().unwrap();
    //         let right = right.into_map().unwrap();
    //         selection_op(op)(left, right) as usize
    //     }
    //     _ => 0,
    // }
}

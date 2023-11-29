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
use common_arrow::arrow::buffer::Buffer;

use crate::selection_op;
use crate::selection_op_ref;
use crate::types::array::ArrayColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::Column;
use crate::Scalar;
use crate::SelectOp;
use crate::SelectStrategy;

pub fn select_scalar_and_column(
    op: SelectOp,
    scalar: Scalar,
    mut column: Column,
    column_data_type: DataType,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    if let Scalar::Null = scalar {
        return 0;
    }

    let validity = if let DataType::Nullable(_) = column_data_type {
        let nullable_column = column.into_nullable().unwrap();
        column = nullable_column.column;
        Some(nullable_column.validity)
    } else {
        None
    };

    match column_data_type.remove_nullable() {
        DataType::Null | DataType::EmptyArray | DataType::EmptyMap => 0,
        DataType::Number(NumberDataType::UInt8) => {
            let scalar = scalar.into_number().unwrap().into_u_int8().unwrap();
            let column = column.into_number().unwrap().into_u_int8().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::UInt16) => {
            let scalar = scalar.into_number().unwrap().into_u_int16().unwrap();
            let column = column.into_number().unwrap().into_u_int16().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::UInt32) => {
            let scalar = scalar.into_number().unwrap().into_u_int32().unwrap();
            let column = column.into_number().unwrap().into_u_int32().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::UInt64) => {
            let scalar = scalar.into_number().unwrap().into_u_int64().unwrap();
            let column = column.into_number().unwrap().into_u_int64().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Int8) => {
            let scalar = scalar.into_number().unwrap().into_int8().unwrap();
            let column = column.into_number().unwrap().into_int8().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Int16) => {
            let scalar = scalar.into_number().unwrap().into_int16().unwrap();
            let column = column.into_number().unwrap().into_int16().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Int32) => {
            let scalar = scalar.into_number().unwrap().into_int32().unwrap();
            let column = column.into_number().unwrap().into_int32().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Int64) => {
            let scalar = scalar.into_number().unwrap().into_int64().unwrap();
            let column = column.into_number().unwrap().into_int64().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Float32) => {
            let scalar = scalar.into_number().unwrap().into_float32().unwrap();
            let column = column.into_number().unwrap().into_float32().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Number(NumberDataType::Float64) => {
            let scalar = scalar.into_number().unwrap().into_float64().unwrap();
            let column = column.into_number().unwrap().into_float64().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let (scalar, _) = scalar.into_decimal().unwrap().into_decimal128().unwrap();
            let (column, _) = column.into_decimal().unwrap().into_decimal128().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let (scalar, _) = scalar.into_decimal().unwrap().into_decimal256().unwrap();
            let (column, _) = column.into_decimal().unwrap().into_decimal256().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Date => {
            let scalar = scalar.into_date().unwrap();
            let column = column.into_date().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Timestamp => {
            let scalar = scalar.into_timestamp().unwrap();
            let column = column.into_timestamp().unwrap();
            select_primitive_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::String => {
            let scalar = scalar.into_string().unwrap();
            let column = column.into_string().unwrap();
            select_string_scalar_and_column_adapt(
                op,
                &scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Variant => {
            let scalar = scalar.into_variant().unwrap();
            let column = column.into_variant().unwrap();
            select_string_scalar_and_column_adapt(
                op,
                &scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Bitmap => {
            let scalar = scalar.into_bitmap().unwrap();
            let column = column.into_bitmap().unwrap();
            select_string_scalar_and_column_adapt(
                op,
                &scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Boolean => {
            let scalar = scalar.into_boolean().unwrap();
            let column = column.into_boolean().unwrap();
            select_boolean_scalar_and_column_adapt(
                op,
                scalar,
                column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Array(_) => {
            let scalar = scalar.into_array().unwrap();
            let column = column.into_array().unwrap();
            select_array_scalar_and_column_adapt(
                op,
                &scalar,
                *column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Map(_) => {
            let scalar = scalar.into_map().unwrap();
            let column = column.into_map().unwrap();
            select_array_scalar_and_column_adapt(
                op,
                &scalar,
                *column,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
        DataType::Tuple(_) => {
            // let scalar = scalar.into_tuple().unwrap();
            // let column = column.into_tuple().unwrap();
            unimplemented!()
        }
        _ => unreachable!("Here is no Nullable(_) and Generic(_)"),
    }
}

pub fn select_primitive_scalar_and_column_adapt<T>(
    op: SelectOp,
    scalar: T,
    column: Buffer<T>,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize
where
    T: std::cmp::PartialOrd + std::fmt::Display,
{
    let has_true = !true_selection.is_empty();
    let has_false = false_selection.1;
    if has_true && has_false {
        select_primitive_scalar_and_column::<_, true, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_primitive_scalar_and_column::<_, true, false>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_primitive_scalar_and_column::<_, false, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn select_string_scalar_and_column_adapt(
    op: SelectOp,
    scalar: &[u8],
    column: StringColumn,
    validity: Option<Bitmap>,
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
        select_string_scalar_and_column::<true, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_string_scalar_and_column::<true, false>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_string_scalar_and_column::<false, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn select_boolean_scalar_and_column_adapt(
    op: SelectOp,
    scalar: bool,
    column: Bitmap,
    validity: Option<Bitmap>,
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
        select_boolean_scalar_and_column::<true, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_boolean_scalar_and_column::<true, false>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_boolean_scalar_and_column::<false, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn select_array_scalar_and_column_adapt(
    op: SelectOp,
    scalar: &Column,
    column: ArrayColumn<AnyType>,
    validity: Option<Bitmap>,
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
        select_array_scalar_and_column::<true, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_array_scalar_and_column::<true, false>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_array_scalar_and_column::<false, true>(
            op,
            scalar,
            column,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

pub fn select_primitive_scalar_and_column<T, const TRUE: bool, const FALSE: bool>(
    op: SelectOp,
    scalar: T,
    column: Buffer<T>,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize
where
    T: std::cmp::PartialOrd + std::fmt::Display,
{
    let op = selection_op_ref::<T>(op);
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(&scalar, column.get_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if op(&scalar, column.get_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(&scalar, column.get_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if op(&scalar, column.get_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::ALL => unsafe {
            match validity {
                Some(validity) => {
                    for idx in 0u32..count as u32 {
                        if validity.get_bit_unchecked(idx as usize)
                            && op(&scalar, column.get_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for idx in 0u32..count as u32 {
                        if op(&scalar, column.get_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
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

pub fn select_string_scalar_and_column<const TRUE: bool, const FALSE: bool>(
    op: SelectOp,
    scalar: &[u8],
    column: StringColumn,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let op = selection_op::<&[u8]>(op);
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if op(scalar, column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if op(scalar, column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::ALL => unsafe {
            match validity {
                Some(validity) => {
                    for idx in 0u32..count as u32 {
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for idx in 0u32..count as u32 {
                        if op(scalar, column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
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

pub fn select_boolean_scalar_and_column<const TRUE: bool, const FALSE: bool>(
    op: SelectOp,
    scalar: bool,
    column: Bitmap,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let op = selection_op::<bool>(op);
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.get_bit_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if op(scalar, column.get_bit_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.get_bit_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if op(scalar, column.get_bit_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::ALL => unsafe {
            match validity {
                Some(validity) => {
                    for idx in 0u32..count as u32 {
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, column.get_bit_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for idx in 0u32..count as u32 {
                        if op(scalar, column.get_bit_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
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

pub fn select_array_scalar_and_column<const TRUE: bool, const FALSE: bool>(
    op: SelectOp,
    scalar: &Column,
    column: ArrayColumn<AnyType>,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let op = selection_op_ref::<Column>(op);
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, &column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = true_selection[i];
                        if op(scalar, &column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, &column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = false_selection[i];
                        if op(scalar, &column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
            }
        },
        SelectStrategy::ALL => unsafe {
            match validity {
                Some(validity) => {
                    for idx in 0u32..count as u32 {
                        if validity.get_bit_unchecked(idx as usize)
                            && op(scalar, &column.index_unchecked(idx as usize))
                        {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
                }
                None => {
                    for idx in 0u32..count as u32 {
                        if op(scalar, &column.index_unchecked(idx as usize)) {
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += 1;
                            }
                        } else if FALSE {
                            false_selection[false_idx] = idx;
                            false_idx += 1;
                        }
                    }
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

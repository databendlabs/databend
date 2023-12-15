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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::filter::selection_op;
use crate::filter::selection_op_boolean;
use crate::filter::selection_op_string;
use crate::filter::selection_op_tuple;
use crate::filter::selection_op_variant;
use crate::filter::tuple_compare_default_value;
use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::array::ArrayColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::Column;
use crate::Scalar;
use crate::ScalarRef;

impl<'a> Selector<'a> {
    // Select indices by comparing scalar and column.
    #[allow(clippy::too_many_arguments)]
    pub fn select_scalar_and_column(
        &self,
        op: &SelectOp,
        scalar: Scalar,
        mut column: Column,
        column_data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        if let Scalar::Null = scalar {
            if false_selection.1 {
                return Ok(self.select_null(
                    true_selection,
                    false_selection.0,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                ));
            } else {
                return Ok(0);
            }
        }

        // Remove NullableColumn and get the inner column and validity.
        let validity = if let DataType::Nullable(_) = column_data_type {
            let nullable_column = column.into_nullable().unwrap();
            column = nullable_column.column;
            Some(nullable_column.validity)
        } else {
            None
        };

        let count = match column_data_type.remove_nullable() {
            DataType::Null | DataType::EmptyMap => 0,
            DataType::EmptyArray => self.select_empty_array_adapt(
                op,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            ),
            DataType::Number(NumberDataType::UInt8) => {
                let scalar = scalar.into_number().unwrap().into_u_int8().unwrap();
                let column = column.into_number().unwrap().into_u_int8().unwrap();
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_primitive_scalar_and_column_adapt(
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
                self.select_bytes_scalar_and_column_adapt(
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
                    false,
                )
            }
            DataType::Variant => {
                let scalar = scalar.into_variant().unwrap();
                let column = column.into_variant().unwrap();
                self.select_bytes_scalar_and_column_adapt(
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
                    true,
                )
            }
            DataType::Boolean => {
                let scalar = scalar.into_boolean().unwrap();
                let column = column.into_boolean().unwrap();
                self.select_boolean_scalar_and_column_adapt(
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
                self.select_array_scalar_and_column_adapt(
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
                let scalar = scalar.into_tuple().unwrap();
                let column = column.into_tuple().unwrap();
                self.select_tuple_scalar_and_column_adapt(
                    op,
                    &scalar,
                    &column,
                    validity,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                )
            }
            _ => {
                // EmptyMap, Map, Bitmap do not support comparison, Nullable has been removed,
                // Generic has been converted to a specific DataType.
                return Err(ErrorCode::UnsupportedDataType(format!(
                    "{:?} is not supported for comparison",
                    &column_data_type
                )));
            }
        };
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    fn select_primitive_scalar_and_column_adapt<T: Copy>(
        &self,
        op: &SelectOp,
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
        T: std::cmp::PartialOrd,
    {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;
        if has_true && has_false {
            self.select_primitive_scalar_and_column::<_, true, true>(
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
            self.select_primitive_scalar_and_column::<_, true, false>(
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
            self.select_primitive_scalar_and_column::<_, false, true>(
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

    #[allow(clippy::too_many_arguments)]
    fn select_bytes_scalar_and_column_adapt(
        &self,
        op: &SelectOp,
        scalar: &[u8],
        column: StringColumn,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
        is_variant: bool,
    ) -> usize {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;
        if has_true && has_false {
            self.select_bytes_scalar_and_column::<true, true>(
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
                is_variant,
            )
        } else if has_true {
            self.select_bytes_scalar_and_column::<true, false>(
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
                is_variant,
            )
        } else {
            self.select_bytes_scalar_and_column::<false, true>(
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
                is_variant,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn select_boolean_scalar_and_column_adapt(
        &self,
        op: &SelectOp,
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
            self.select_boolean_scalar_and_column::<true, true>(
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
            self.select_boolean_scalar_and_column::<true, false>(
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
            self.select_boolean_scalar_and_column::<false, true>(
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

    #[allow(clippy::too_many_arguments)]
    fn select_array_scalar_and_column_adapt(
        &self,
        op: &SelectOp,
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
            self.select_array_scalar_and_column::<true, true>(
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
            self.select_array_scalar_and_column::<true, false>(
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
            self.select_array_scalar_and_column::<false, true>(
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

    #[allow(clippy::too_many_arguments)]
    fn select_tuple_scalar_and_column_adapt(
        &self,
        op: &SelectOp,
        scalar: &[Scalar],
        column: &[Column],
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
            self.select_tuple_scalar_and_column::<true, true>(
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
            self.select_tuple_scalar_and_column::<true, false>(
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
            self.select_tuple_scalar_and_column::<false, true>(
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

    #[allow(clippy::too_many_arguments)]
    fn select_primitive_scalar_and_column<T: Copy, const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
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
        T: std::cmp::PartialOrd,
    {
        let op = selection_op::<T>(op);
        let mut true_idx = *true_start_idx;
        let mut false_idx = *false_start_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *true_start_idx;
                let end = *true_start_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
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
                            let idx = *false_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = op(scalar, *column.get_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
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

    #[allow(clippy::too_many_arguments)]
    fn select_bytes_scalar_and_column<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        scalar: &[u8],
        column: StringColumn,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        true_start_idx: &mut usize,
        false_start_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
        is_variant: bool,
    ) -> usize {
        let op = if is_variant {
            selection_op_variant(op)
        } else {
            selection_op_string(op)
        };
        let mut true_idx = *true_start_idx;
        let mut false_idx = *false_start_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *true_start_idx;
                let end = *true_start_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
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
                            let idx = *false_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = op(scalar, column.index_unchecked(idx as usize));
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
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

    #[allow(clippy::too_many_arguments)]
    fn select_boolean_scalar_and_column<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
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
        let op = selection_op_boolean(op);
        let mut true_idx = *true_start_idx;
        let mut false_idx = *false_start_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *true_start_idx;
                let end = *true_start_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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
                            let idx = *false_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = op(scalar, column.get_bit_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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

    #[allow(clippy::too_many_arguments)]
    fn select_array_scalar_and_column<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
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
        let op = selection_op::<Column>(op);
        let mut true_idx = *true_start_idx;
        let mut false_idx = *false_start_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *true_start_idx;
                let end = *true_start_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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
                            let idx = *false_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = op(scalar.clone(), column.index_unchecked(idx as usize));
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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

    #[allow(clippy::too_many_arguments)]
    fn select_tuple_scalar_and_column<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        scalar: &[Scalar],
        column: &[Column],
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        true_start_idx: &mut usize,
        false_start_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let default = tuple_compare_default_value(op);
        let op = selection_op_tuple::<ScalarRef>(op);
        let mut true_idx = *true_start_idx;
        let mut false_idx = *false_start_idx;
        let scalar = scalar
            .iter()
            .map(|scalar| scalar.as_ref())
            .collect::<Vec<_>>();
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *true_start_idx;
                let end = *true_start_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs, rhs_field) in scalar.iter().zip(column) {
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs.clone(), rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let mut ret = default;
                            for (lhs, rhs_field) in scalar.iter().zip(column) {
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs.clone(), rhs) {
                                    ret = result;
                                    break;
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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
                            let idx = *false_selection.get_unchecked(i);
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs, rhs_field) in scalar.iter().zip(column) {
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs.clone(), rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let mut ret = default;
                            for (lhs, rhs_field) in scalar.iter().zip(column) {
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs.clone(), rhs) {
                                    ret = result;
                                    break;
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs, rhs_field) in scalar.iter().zip(column) {
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs.clone(), rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let mut ret = default;
                            for (lhs, rhs_field) in scalar.iter().zip(column) {
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs.clone(), rhs) {
                                    ret = result;
                                    break;
                                }
                            }
                            if TRUE {
                                true_selection[true_idx] = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                false_selection[false_idx] = idx;
                                false_idx += !ret as usize;
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
}

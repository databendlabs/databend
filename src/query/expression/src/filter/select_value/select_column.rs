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

use crate::arrow::and_validities;
use crate::filter::boolean_selection_op;
use crate::filter::selection_op;
use crate::filter::string_selection_op;
use crate::filter::tuple_compare_default_value;
use crate::filter::tuple_selection_op;
use crate::filter::variant_selection_op;
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
use crate::ScalarRef;

impl<'a> Selector<'a> {
    // Select indices by comparing two columns.
    #[allow(clippy::too_many_arguments)]
    pub fn select_columns(
        &self,
        op: &SelectOp,
        mut left: Column,
        mut right: Column,
        left_data_type: DataType,
        right_data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        // Remove Nullable(_) and get the inner column and validity.
        let mut validity = None;
        if let DataType::Nullable(_) = left_data_type {
            let nullable_left = left.into_nullable().unwrap();
            left = nullable_left.column;
            validity = and_validities(Some(nullable_left.validity), validity);
        }
        if let DataType::Nullable(_) = right_data_type {
            let nullable_right = right.into_nullable().unwrap();
            right = nullable_right.column;
            validity = and_validities(Some(nullable_right.validity), validity);
        }

        match left_data_type.remove_nullable() {
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
                let left = left.into_number().unwrap().into_u_int8().unwrap();
                let right = right.into_number().unwrap().into_u_int8().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_u_int16().unwrap();
                let right = right.into_number().unwrap().into_u_int16().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_u_int32().unwrap();
                let right = right.into_number().unwrap().into_u_int32().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_u_int64().unwrap();
                let right = right.into_number().unwrap().into_u_int64().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_int8().unwrap();
                let right = right.into_number().unwrap().into_int8().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_int16().unwrap();
                let right = right.into_number().unwrap().into_int16().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_int32().unwrap();
                let right = right.into_number().unwrap().into_int32().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_int64().unwrap();
                let right = right.into_number().unwrap().into_int64().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_float32().unwrap();
                let right = right.into_number().unwrap().into_float32().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_number().unwrap().into_float64().unwrap();
                let right = right.into_number().unwrap().into_float64().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let (left, _) = left.into_decimal().unwrap().into_decimal128().unwrap();
                let (right, _) = right.into_decimal().unwrap().into_decimal128().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let (left, _) = left.into_decimal().unwrap().into_decimal256().unwrap();
                let (right, _) = right.into_decimal().unwrap().into_decimal256().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_date().unwrap();
                let right = right.into_date().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_timestamp().unwrap();
                let right = right.into_timestamp().unwrap();
                self.select_primitive_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_string().unwrap();
                let right = right.into_string().unwrap();
                self.select_bytes_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_variant().unwrap();
                let right = right.into_variant().unwrap();
                self.select_bytes_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_boolean().unwrap();
                let right = right.into_boolean().unwrap();
                self.select_boolean_adapt(
                    op,
                    left,
                    right,
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
                let left = left.into_array().unwrap();
                let right = right.into_array().unwrap();
                self.select_array_adapt(
                    op,
                    *left,
                    *right,
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
                let left = left.into_map().unwrap();
                let right = right.into_map().unwrap();
                self.select_array_adapt(
                    op,
                    *left,
                    *right,
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
                let left = left.into_tuple().unwrap();
                let right = right.into_tuple().unwrap();
                self.select_tuple_adapt(
                    op,
                    &left,
                    &right,
                    validity,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                )
            }
            _ => unreachable!("Here is no Nullable(_) or Generic(_)"),
        }
    }

    pub fn select_boolean_column_adapt(
        &self,
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
            self.select_boolean_column::<true, true>(
                column,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_boolean_column::<true, false>(
                column,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean_column::<false, true>(
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

    #[allow(clippy::too_many_arguments)]
    fn select_primitive_adapt<T>(
        &self,
        op: &SelectOp,
        left: Buffer<T>,
        right: Buffer<T>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize
    where
        T: std::cmp::PartialOrd + Copy,
    {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;
        if has_true && has_false {
            self.select_primitive::<_, true, true>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_primitive::<_, true, false>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_primitive::<_, false, true>(
                op,
                left,
                right,
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
    fn select_bytes_adapt(
        &self,
        op: &SelectOp,
        left: StringColumn,
        right: StringColumn,
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
            self.select_bytes::<true, true>(
                op,
                left,
                right,
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
            self.select_bytes::<true, false>(
                op,
                left,
                right,
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
            self.select_bytes::<false, true>(
                op,
                left,
                right,
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
    fn select_boolean_adapt(
        &self,
        op: &SelectOp,
        left: Bitmap,
        right: Bitmap,
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
            self.select_boolean::<true, true>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_boolean::<true, false>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean::<false, true>(
                op,
                left,
                right,
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
    fn select_array_adapt(
        &self,
        op: &SelectOp,
        left: ArrayColumn<AnyType>,
        right: ArrayColumn<AnyType>,
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
            self.select_array::<true, true>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_array::<true, false>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_array::<false, true>(
                op,
                left,
                right,
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
    fn select_tuple_adapt(
        &self,
        op: &SelectOp,
        left: &[Column],
        right: &[Column],
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let has_true = !true_selection.is_empty();
        let has_false = !false_selection.1;
        if has_true && has_false {
            self.select_tuple::<true, true>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_tuple::<true, false>(
                op,
                left,
                right,
                validity,
                true_selection,
                false_selection.0,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_tuple::<false, true>(
                op,
                left,
                right,
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
    fn select_primitive<T, const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        left: Buffer<T>,
        right: Buffer<T>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        true_start_idx: &mut usize,
        false_start_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize
    where
        T: std::cmp::PartialOrd + Copy,
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
                                && op(
                                    *left.get_unchecked(idx as usize),
                                    *right.get_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                *left.get_unchecked(idx as usize),
                                *right.get_unchecked(idx as usize),
                            );
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
                                && op(
                                    *left.get_unchecked(idx as usize),
                                    *right.get_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                *left.get_unchecked(idx as usize),
                                *right.get_unchecked(idx as usize),
                            );
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
                                && op(
                                    *left.get_unchecked(idx as usize),
                                    *right.get_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                *left.get_unchecked(idx as usize),
                                *right.get_unchecked(idx as usize),
                            );
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
    fn select_bytes<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        left: StringColumn,
        right: StringColumn,
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
            variant_selection_op(op)
        } else {
            string_selection_op(op)
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
    fn select_boolean<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        left: Bitmap,
        right: Bitmap,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        true_start_idx: &mut usize,
        false_start_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let op = boolean_selection_op(op);
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
                                && op(
                                    left.get_bit_unchecked(idx as usize),
                                    right.get_bit_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.get_bit_unchecked(idx as usize),
                                right.get_bit_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.get_bit_unchecked(idx as usize),
                                    right.get_bit_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.get_bit_unchecked(idx as usize),
                                right.get_bit_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.get_bit_unchecked(idx as usize),
                                    right.get_bit_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.get_bit_unchecked(idx as usize),
                                right.get_bit_unchecked(idx as usize),
                            );
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
    fn select_array<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        left: ArrayColumn<AnyType>,
        right: ArrayColumn<AnyType>,
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
                                && op(
                                    left.index_unchecked(idx as usize),
                                    right.index_unchecked(idx as usize),
                                );
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
                            let ret = op(
                                left.index_unchecked(idx as usize),
                                right.index_unchecked(idx as usize),
                            );
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
    fn select_tuple<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        left: &[Column],
        right: &[Column],
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        true_start_idx: &mut usize,
        false_start_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let default = tuple_compare_default_value(op);
        let op = tuple_selection_op::<ScalarRef>(op);
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
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs_field, rhs_field) in left.iter().zip(right) {
                                    let lhs = lhs_field.index(idx as usize).unwrap();
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs, rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
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
                            let mut ret = default;
                            for (lhs_field, rhs_field) in left.iter().zip(right) {
                                let lhs = lhs_field.index(idx as usize).unwrap();
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs, rhs) {
                                    ret = result;
                                    break;
                                }
                            }
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
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs_field, rhs_field) in left.iter().zip(right) {
                                    let lhs = lhs_field.index(idx as usize).unwrap();
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs, rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
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
                            let mut ret = default;
                            for (lhs_field, rhs_field) in left.iter().zip(right) {
                                let lhs = lhs_field.index(idx as usize).unwrap();
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs, rhs) {
                                    ret = result;
                                    break;
                                }
                            }
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
                            let mut ret = validity.get_bit_unchecked(idx as usize);
                            if ret {
                                ret = default;
                                for (lhs_field, rhs_field) in left.iter().zip(right) {
                                    let lhs = lhs_field.index(idx as usize).unwrap();
                                    let rhs = rhs_field.index(idx as usize).unwrap();
                                    if let Some(result) = op(lhs, rhs) {
                                        ret = result;
                                        break;
                                    }
                                }
                            }
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
                            let mut ret = default;
                            for (lhs_field, rhs_field) in left.iter().zip(right) {
                                let lhs = lhs_field.index(idx as usize).unwrap();
                                let rhs = rhs_field.index(idx as usize).unwrap();
                                if let Some(result) = op(lhs, rhs) {
                                    ret = result;
                                    break;
                                }
                            }
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

    fn select_boolean_column<const TRUE: bool, const FALSE: bool>(
        &self,
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
            SelectStrategy::All => unsafe {
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
}

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

use bumpalo::Bump;
use databend_common_column::bitmap::Bitmap;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::bincode_serialize_into_buf;

use super::probe_state::ProbeState;
use super::row_ptr::RowLayout;
use super::row_ptr::RowPtr;
use crate::types::decimal::DecimalColumn;
use crate::types::i256;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::BinaryType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataKind;
use crate::types::DecimalView;
use crate::types::NumberColumn;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::SelectVector;
use crate::BATCH_SIZE;

pub(super) fn rowformat_size(data_type: &DataType) -> usize {
    match data_type {
        DataType::Null | DataType::EmptyArray | DataType::EmptyMap => 0,
        DataType::Boolean => 1,
        DataType::Number(n) => n.bit_width() as usize / 8,
        DataType::Decimal(size) => {
            if size.can_carried_by_64() {
                8
            } else if size.can_carried_by_128() {
                16
            } else {
                32
            }
        }
        DataType::Timestamp => 8,
        DataType::Date => 4,
        DataType::Interval => 16,
        // use address instead
        DataType::Binary
        | DataType::String
        | DataType::Bitmap
        | DataType::Variant
        | DataType::Geometry
        | DataType::Geography => 4 + 8, // u32 len + address
        DataType::Nullable(x) => rowformat_size(x),
        DataType::Array(_) | DataType::Map(_) | DataType::Tuple(_) | DataType::Vector(_) => 4 + 8,
        DataType::Generic(_) => unreachable!(),
        DataType::Opaque(size) => size * 8,
    }
}

/// This serialize column into row format by fixed size
pub(super) unsafe fn serialize_column_to_rowformat(
    arena: &Bump,
    column: &Column,
    select_vector: &SelectVector,
    rows: usize,
    address: &[RowPtr; BATCH_SIZE],
    offset: usize,
    scratch: &mut Vec<u8>,
) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(buffer) => {
                for index in select_vector.iter().take(rows).copied() {
                    address[index].write(offset, &buffer[index]);
                }
            }
        }),
        Column::Decimal(decimal_column) => {
            with_decimal_mapped_type!(|F| match decimal_column {
                DecimalColumn::F(buffer, size) => {
                    with_decimal_mapped_type!(|T| match size.data_kind() {
                        DecimalDataKind::T => {
                            serialize_fixed_size_column_to_rowformat::<DecimalView<F, T>>(
                                buffer,
                                &select_vector[0..rows],
                                address,
                                offset,
                            );
                        }
                    });
                }
            });
        }
        Column::Boolean(v) => {
            if v.null_count() == 0 || v.null_count() == v.len() {
                let val: u8 = if v.null_count() == 0 { 1 } else { 0 };
                // faster path
                for index in select_vector.iter().take(rows).copied() {
                    address[index].write_u8(offset, val);
                }
            } else {
                for index in select_vector.iter().take(rows).copied() {
                    address[index].write_u8(offset, v.get_bit(index) as u8);
                }
            }
        }
        Column::Binary(v) | Column::Bitmap(v) | Column::Variant(v) | Column::Geometry(v) => {
            for index in select_vector.iter().take(rows).copied() {
                let data = arena.alloc_slice_copy(v.index_unchecked(index));
                address[index].write_bytes(offset, data);
            }
        }
        Column::String(v) => {
            for index in select_vector.iter().take(rows).copied() {
                let data = arena.alloc_str(v.index_unchecked(index));
                address[index].write_bytes(offset, data.as_bytes());
            }
        }
        Column::Timestamp(buffer) => {
            for index in select_vector.iter().take(rows).copied() {
                address[index].write(offset, &buffer[index]);
            }
        }
        Column::Date(buffer) => {
            for index in select_vector.iter().take(rows).copied() {
                address[index].write(offset, &buffer[index]);
            }
        }
        Column::Nullable(c) => serialize_column_to_rowformat(
            arena,
            &c.column,
            select_vector,
            rows,
            address,
            offset,
            scratch,
        ),

        // for complex column
        other => {
            for index in select_vector.iter().take(rows).copied() {
                let s = other.index_unchecked(index).to_owned();
                scratch.clear();
                bincode_serialize_into_buf(scratch, &s).unwrap();

                let data = arena.alloc_slice_copy(scratch);
                address[index].write_bytes(offset, data);
            }
        }
    }
}

unsafe fn serialize_fixed_size_column_to_rowformat<T>(
    column: &T::Column,
    select_vector: &[usize],
    address: &[RowPtr; BATCH_SIZE],
    offset: usize,
) where
    T: AccessType<Scalar: Copy>,
{
    for index in select_vector.iter().copied() {
        let val = T::index_column_unchecked_scalar(column, index);
        address[index].write(offset, &val);
    }
}

impl ProbeState {
    pub(super) fn row_match_columns(
        &mut self,
        cols: ProjectedBlock,
        row_layout: &RowLayout,
        (mut count, mut no_match_count): (usize, usize),
    ) -> usize {
        for ((entry, col_offset), validity_offset) in cols
            .iter()
            .zip(row_layout.group_offsets.iter())
            .zip(row_layout.validity_offsets.iter())
        {
            (count, no_match_count) = self.row_match_column(
                &entry.to_column(),
                *validity_offset,
                *col_offset,
                (count, no_match_count),
            );

            // no row matches
            if count == 0 {
                return no_match_count;
            }
        }
        no_match_count
    }

    fn row_match_column(
        &mut self,
        col: &Column,
        validity_offset: usize,
        col_offset: usize,
        (count, no_match_count): (usize, usize),
    ) -> (usize, usize) {
        let (validity, col) = if let Column::Nullable(c) = col {
            (Some(&c.validity), &c.column)
        } else {
            (None, col)
        };

        match col {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                (no_match_count, no_match_count)
            }

            Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberColumn::NUM_TYPE(_) => {
                    self.row_match_column_type::<NumberType<NUM_TYPE>>(
                        col,
                        validity,
                        validity_offset,
                        col_offset,
                        (count, no_match_count),
                    )
                }
            }),
            Column::Decimal(decimal_column) => {
                with_decimal_mapped_type!(|F| match decimal_column {
                    DecimalColumn::F(_, size) => {
                        with_decimal_mapped_type!(|T| match size.data_kind() {
                            DecimalDataKind::T => {
                                self.row_match_column_type::<DecimalView<F, T>>(
                                    col,
                                    validity,
                                    validity_offset,
                                    col_offset,
                                    (count, no_match_count),
                                )
                            }
                        })
                    }
                })
            }
            Column::Boolean(_) => self.row_match_column_type::<BooleanType>(
                col,
                validity,
                validity_offset,
                col_offset,
                (count, no_match_count),
            ),
            Column::Timestamp(_) => self.row_match_column_type::<TimestampType>(
                col,
                validity,
                validity_offset,
                col_offset,
                (count, no_match_count),
            ),
            Column::Date(_) => self.row_match_column_type::<DateType>(
                col,
                validity,
                validity_offset,
                col_offset,
                (count, no_match_count),
            ),
            Column::String(v) => self.row_match_column_generic(
                validity,
                validity_offset,
                (count, no_match_count),
                |idx, row_ptr| unsafe {
                    let value = StringType::index_column_unchecked(v, idx);
                    row_ptr.is_bytes_eq(col_offset, value.as_bytes())
                },
            ),
            Column::Bitmap(v) | Column::Binary(v) | Column::Variant(v) | Column::Geometry(v) => {
                self.row_match_column_generic(
                    validity,
                    validity_offset,
                    (count, no_match_count),
                    |idx, row_ptr| unsafe {
                        let value = BinaryType::index_column_unchecked(v, idx);
                        row_ptr.is_bytes_eq(col_offset, value)
                    },
                )
            }
            Column::Nullable(_) => unreachable!("nullable is unwrapped"),
            other => self.row_match_generic_column(other, col_offset, (count, no_match_count)),
        }
    }

    fn row_match_column_type<T>(
        &mut self,
        col: &Column,
        validity: Option<&Bitmap>,
        validity_offset: usize,
        col_offset: usize,
        (count, no_match_count): (usize, usize),
    ) -> (usize, usize)
    where
        T: AccessType,
        for<'a, 'b> T::ScalarRef<'a>: PartialEq<T::ScalarRef<'b>>,
    {
        let col = T::try_downcast_column(col).unwrap();

        self.row_match_column_generic(
            validity,
            validity_offset,
            (count, no_match_count),
            |idx, row_ptr| unsafe {
                let value = T::index_column_unchecked(&col, idx);
                let scalar = row_ptr.read::<T::Scalar>(col_offset);
                let scalar = T::to_scalar_ref(&scalar);
                scalar == value
            },
        )
    }

    fn row_match_column_generic<F>(
        &mut self,
        validity: Option<&Bitmap>,
        validity_offset: usize,
        (count, mut no_match_count): (usize, usize),
        compare_fn: F,
    ) -> (usize, usize)
    where
        F: Fn(usize, &RowPtr) -> bool,
    {
        let mut match_count = 0;

        if let Some(validity) = validity {
            let is_all_set = validity.null_count() == 0;
            for idx in self.group_compare_vector[..count].iter().copied() {
                let is_set2 = unsafe { self.addresses[idx].read::<u8>(validity_offset) != 0 };
                let is_set = is_all_set || unsafe { validity.get_bit_unchecked(idx) };

                let equal = if is_set && is_set2 {
                    compare_fn(idx, &self.addresses[idx])
                } else {
                    is_set == is_set2
                };

                if equal {
                    self.temp_vector[match_count] = idx;
                    match_count += 1;
                } else {
                    self.no_match_vector[no_match_count] = idx;
                    no_match_count += 1;
                }
            }
        } else {
            for idx in self.group_compare_vector[..count].iter().copied() {
                if compare_fn(idx, &self.addresses[idx]) {
                    self.temp_vector[match_count] = idx;
                    match_count += 1;
                } else {
                    self.no_match_vector[no_match_count] = idx;
                    no_match_count += 1;
                }
            }
        }

        self.group_compare_vector[..match_count].clone_from_slice(&self.temp_vector[..match_count]);
        (match_count, no_match_count)
    }

    fn row_match_generic_column(
        &mut self,
        col: &Column,
        col_offset: usize,
        (count, mut no_match_count): (usize, usize),
    ) -> (usize, usize) {
        let mut match_count = 0;

        for idx in self.group_compare_vector[..count].iter().copied() {
            let value = unsafe { AnyType::index_column_unchecked(col, idx) };
            let scalar = unsafe { self.addresses[idx].read_bytes(col_offset) };
            let scalar: Scalar = bincode_deserialize_from_slice(scalar).unwrap();

            if scalar.as_ref() == value {
                self.temp_vector[match_count] = idx;
                match_count += 1;
            } else {
                self.no_match_vector[no_match_count] = idx;
                no_match_count += 1;
            }
        }
        self.group_compare_vector[..match_count].clone_from_slice(&self.temp_vector[..match_count]);
        (match_count, no_match_count)
    }
}

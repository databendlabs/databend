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
use databend_common_base::hints::assume;
use databend_common_column::bitmap::Bitmap;
use databend_common_io::deserialize_bitmap;
use databend_common_io::deserialize_bitmap;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::bincode_serialize_into_buf;

use super::RowID;
use super::RowLayout;
use super::RowPtr;
use crate::BATCH_SIZE;
use crate::BlockEntry;
use crate::Column;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::BinaryType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataKind;
use crate::types::DecimalScalar;
use crate::types::DecimalView;
use crate::types::NumberColumn;
use crate::types::NumberScalar;
use crate::types::NumberType;
use crate::types::TimestampType;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::i256;
use crate::utils::bitmap::is_hybrid_encoding;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;

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
        DataType::TimestampTz => 16,
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
        DataType::Generic(_) | DataType::StageLocation => unreachable!(),
        DataType::Opaque(size) => size * 8,
    }
}

/// This serialize column into row format by fixed size
pub(super) unsafe fn serialize_column_to_rowformat(
    arena: &Bump,
    column: &Column,
    select_vector: &[RowID],
    address: &mut [RowPtr; BATCH_SIZE],
    offset: usize,
    scratch: &mut Vec<u8>,
) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(buffer) => {
                for row in select_vector {
                    address[*row].write(offset, &buffer[*row]);
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
                                select_vector,
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
                for row in select_vector {
                    unsafe {
                        address[*row].write_u8(offset, val);
                    }
                }
            } else {
                for row in select_vector {
                    unsafe {
                        address[*row].write_u8(offset, v.get_bit(row.to_usize()) as u8);
                    }
                }
            }
        }
        Column::Bitmap(v) => {
            for &index in select_vector {
                let value = unsafe { v.index_unchecked(index.to_usize()) };
                let normalized = if is_hybrid_encoding(value) {
                    value
                } else {
                    match deserialize_bitmap(value) {
                        Ok(bitmap) => {
                            scratch.clear();
                            // Safe unwrap: serialize_into writes into Vec<u8>.
                            bitmap.serialize_into(&mut *scratch).unwrap();
                            scratch.as_slice()
                        }
                        Err(_) => value,
                    }
                };
                let data = arena.alloc_slice_copy(normalized);
                address[index].write_bytes(offset, data);
            }
        }
        Column::Binary(v) | Column::Variant(v) | Column::Geometry(v) => {
            for &index in select_vector {
                let data = arena.alloc_slice_copy(unsafe { v.index_unchecked(index.to_usize()) });
                address[index].write_bytes(offset, data);
            }
        }
        Column::String(v) => {
            for row in select_vector {
                let data = arena.alloc_str(unsafe { v.index_unchecked(row.to_usize()) });
                unsafe {
                    address[*row].write_bytes(offset, data.as_bytes());
                }
            }
        }
        Column::Timestamp(buffer) => {
            for row in select_vector {
                unsafe {
                    address[*row].write(offset, &buffer[*row]);
                }
            }
        }
        Column::Date(buffer) => {
            for row in select_vector {
                unsafe {
                    address[*row].write(offset, &buffer[*row]);
                }
            }
        }
        Column::Nullable(c) => unsafe {
            serialize_column_to_rowformat(arena, &c.column, select_vector, address, offset, scratch)
        },

        // for complex column
        other => {
            for row in select_vector {
                let s = unsafe { other.index_unchecked(row.to_usize()) }.to_owned();
                scratch.clear();
                bincode_serialize_into_buf(scratch, &s).unwrap();

                let data = arena.alloc_slice_copy(scratch);
                unsafe {
                    address[*row].write_bytes(offset, data);
                }
            }
        }
    }
}

pub(super) unsafe fn serialize_const_column_to_rowformat(
    arena: &Bump,
    scalar: &Scalar,
    data_type: &DataType,
    select_vector: &[RowID],
    address: &mut [RowPtr; BATCH_SIZE],
    offset: usize,
    scratch: &mut Vec<u8>,
) {
    unsafe {
        match scalar {
            Scalar::Null => {
                if let Some(box data_type) = data_type.as_nullable() {
                    serialize_const_column_to_rowformat(
                        arena,
                        &Scalar::default_value(data_type),
                        data_type,
                        select_vector,
                        address,
                        offset,
                        scratch,
                    )
                }
            }
            Scalar::EmptyArray | Scalar::EmptyMap => (),
            Scalar::Number(number_scalar) => {
                with_number_mapped_type!(|NUM_TYPE| match number_scalar {
                    NumberScalar::NUM_TYPE(value) => {
                        for row in select_vector {
                            address[*row].write(offset, value);
                        }
                    }
                })
            }
            Scalar::Decimal(decimal_scalar) => {
                let size = decimal_scalar.size();
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => {
                        let value: T = decimal_scalar.as_decimal();
                        for row in select_vector {
                            address[*row].write(offset, &value);
                        }
                    }
                })
            }
            Scalar::Boolean(value) => {
                let value = if *value { 1 } else { 0 };
                for row in select_vector {
                    address[*row].write_u8(offset, value);
                }
            }
            Scalar::Timestamp(value) => {
                for row in select_vector {
                    address[*row].write(offset, value);
                }
            }
            Scalar::Date(value) => {
                for row in select_vector {
                    address[*row].write(offset, value);
                }
            }
            Scalar::Interval(value) => {
                for row in select_vector {
                    address[*row].write(offset, value);
                }
            }
            Scalar::String(value) => {
                let data = arena.alloc_str(value);
                let bytes = data.as_bytes();
                for row in select_vector {
                    address[*row].write_bytes(offset, bytes);
                }
            }
            Scalar::Binary(value)
            | Scalar::Bitmap(value)
            | Scalar::Variant(value)
            | Scalar::Geometry(value) => {
                let data = arena.alloc_slice_copy(value);
                for row in select_vector {
                    address[*row].write_bytes(offset, data);
                }
            }
            other => {
                scratch.clear();
                bincode_serialize_into_buf(scratch, other).unwrap();
                let data = arena.alloc_slice_copy(scratch);
                for row in select_vector {
                    address[*row].write_bytes(offset, data);
                }
            }
        }
    }
}

unsafe fn serialize_fixed_size_column_to_rowformat<T>(
    column: &T::Column,
    select_vector: &[RowID],
    address: &mut [RowPtr; BATCH_SIZE],
    offset: usize,
) where
    T: AccessType<Scalar: Copy>,
{
    unsafe {
        for row in select_vector {
            let val = T::index_column_unchecked_scalar(column, row.to_usize());
            address[*row].write(offset, &val);
        }
    }
}

pub struct CompareState<'a> {
    pub(super) address: &'a [RowPtr; BATCH_SIZE],
    pub(super) compare: &'a mut [RowID; BATCH_SIZE],
    pub(super) no_matched: &'a mut [RowID; BATCH_SIZE],
}

impl<'s> CompareState<'s> {
    pub(super) fn row_match_entries(
        mut self,
        entries: ProjectedBlock,
        row_layout: &RowLayout,
        (mut count, mut no_match_count): (usize, usize),
    ) -> usize {
        for ((entry, col_offset), validity_offset) in entries
            .iter()
            .zip(row_layout.group_offsets.iter())
            .zip(row_layout.validity_offsets.iter())
        {
            if matches!(
                entry.data_type(),
                DataType::Null | DataType::EmptyMap | DataType::EmptyArray
            ) {
                continue;
            }

            (count, no_match_count) = self.match_entry(
                entry,
                *col_offset,
                *validity_offset,
                (count, no_match_count),
            );

            // no row matches
            if count == 0 {
                return no_match_count;
            }
        }
        no_match_count
    }

    fn match_entry(
        &mut self,
        entry: &BlockEntry,
        col_offset: usize,
        validity_offset: usize,
        counts: (usize, usize),
    ) -> (usize, usize) {
        match entry {
            BlockEntry::Const(scalar, DataType::Nullable(_), _) => {
                if scalar.is_null() {
                    self.match_with(counts, |_, row_ptr| unsafe {
                        !row_ptr.read_bool(validity_offset)
                    })
                } else {
                    let counts = self.match_with(counts, |_, row_ptr| unsafe {
                        row_ptr.read_bool(validity_offset)
                    });
                    self.match_const_column(scalar, col_offset, counts)
                }
            }
            BlockEntry::Const(scalar, _, _) => self.match_const_column(scalar, col_offset, counts),
            BlockEntry::Column(column) => match column {
                Column::Nullable(c) => {
                    if c.validity.null_count() == 0 {
                        let counts = self.match_with(counts, |_, row_ptr| unsafe {
                            row_ptr.read_bool(validity_offset)
                        });
                        self.match_column(&c.column, col_offset, None, counts)
                    } else if c.validity.true_count() == 0 {
                        self.match_with(counts, |_, row_ptr| unsafe {
                            !row_ptr.read_bool(validity_offset)
                        })
                    } else {
                        self.match_column(
                            &c.column,
                            col_offset,
                            Some((&c.validity, validity_offset)),
                            counts,
                        )
                    }
                }
                column => self.match_column(column, col_offset, None, counts),
            },
        }
    }

    fn match_column(
        &mut self,
        col: &Column,
        col_offset: usize,
        validity: Option<(&Bitmap, usize)>,
        counts: (usize, usize),
    ) -> (usize, usize) {
        match col {
            Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.match_validity_with(counts, validity, |_, _| true)
            }
            Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberColumn::NUM_TYPE(buffer) => {
                    self.match_column_type::<NumberType<NUM_TYPE>>(
                        buffer, col_offset, validity, counts,
                    )
                }
            }),
            Column::Decimal(decimal_column) => {
                with_decimal_mapped_type!(|F| match decimal_column {
                    DecimalColumn::F(buffer, size) => {
                        with_decimal_mapped_type!(|T| match size.data_kind() {
                            DecimalDataKind::T => {
                                self.match_column_type::<DecimalView<F, T>>(
                                    buffer, col_offset, validity, counts,
                                )
                            }
                        })
                    }
                })
            }
            Column::Boolean(v) => {
                self.match_column_type::<BooleanType>(v, col_offset, validity, counts)
            }
            Column::Timestamp(buffer) => {
                self.match_column_type::<TimestampType>(buffer, col_offset, validity, counts)
            }
            Column::Date(buffer) => {
                self.match_column_type::<DateType>(buffer, col_offset, validity, counts)
            }
            Column::String(str_view) => {
                self.match_validity_with(counts, validity, |row, row_ptr| unsafe {
                    row_ptr.eq_string_view(col_offset, str_view, *row)
                })
            }
            Column::Bitmap(v) | Column::Binary(v) | Column::Variant(v) | Column::Geometry(v) => {
                self.match_validity_with(counts, validity, |row, row_ptr| unsafe {
                    let value = BinaryType::index_column_unchecked(v, row.to_usize());
                    row_ptr.is_bytes_eq(col_offset, value)
                })
            }
            Column::Nullable(_) | Column::Null { .. } => unreachable!(),
            column => self.match_validity_with(counts, validity, |row, row_ptr| {
                let value = unsafe { AnyType::index_column_unchecked(column, row.to_usize()) };
                let scalar = unsafe { row_ptr.read_bytes(col_offset) };
                let scalar: Scalar = bincode_deserialize_from_slice(scalar).unwrap();

                scalar.as_ref() == value
            }),
        }
    }

    fn match_const_column(
        &mut self,
        scalar: &Scalar,
        col_offset: usize,
        counts: (usize, usize),
    ) -> (usize, usize) {
        match scalar {
            Scalar::Null => unreachable!(),
            Scalar::Number(scalar) => with_number_mapped_type!(|NUM_TYPE| match scalar {
                NumberScalar::NUM_TYPE(scalar) => {
                    self.match_scalar_type::<NumberType<NUM_TYPE>>(scalar, col_offset, counts)
                }
            }),
            Scalar::Decimal(scalar) => {
                with_decimal_mapped_type!(|F| match scalar {
                    DecimalScalar::F(scalar, size) => {
                        with_decimal_mapped_type!(|T| match size.data_kind() {
                            DecimalDataKind::T => {
                                self.match_scalar_type::<DecimalView<F, T>>(
                                    &scalar.as_decimal(),
                                    col_offset,
                                    counts,
                                )
                            }
                        })
                    }
                })
            }
            Scalar::Boolean(value) => {
                self.match_scalar_type::<BooleanType>(value, col_offset, counts)
            }
            Scalar::Timestamp(value) => {
                self.match_scalar_type::<TimestampType>(value, col_offset, counts)
            }
            Scalar::Date(value) => self.match_scalar_type::<DateType>(value, col_offset, counts),
            Scalar::String(value) => self.match_with(counts, |_, row_ptr| unsafe {
                row_ptr.is_bytes_eq(col_offset, value.as_bytes())
            }),
            Scalar::Bitmap(v) | Scalar::Binary(v) | Scalar::Variant(v) | Scalar::Geometry(v) => {
                self.match_with(counts, |_, row_ptr| unsafe {
                    row_ptr.is_bytes_eq(col_offset, v)
                })
            }
            _ => self.match_with(counts, |_, row_ptr| {
                let row_data = unsafe { row_ptr.read_bytes(col_offset) };
                let stored: Scalar = bincode_deserialize_from_slice(row_data).unwrap();
                &stored == scalar
            }),
        }
    }

    fn match_scalar_type<T>(
        &mut self,
        value: &T::Scalar,
        col_offset: usize,
        counts: (usize, usize),
    ) -> (usize, usize)
    where
        T: AccessType,
    {
        self.match_with(counts, |_, row_ptr| unsafe {
            let scalar = row_ptr.read::<T::Scalar>(col_offset);
            scalar == *value
        })
    }

    fn match_column_type<T>(
        &mut self,
        col: &T::Column,
        col_offset: usize,
        validity: Option<(&Bitmap, usize)>,
        counts: (usize, usize),
    ) -> (usize, usize)
    where
        T: AccessType,
        for<'a, 'b> T::ScalarRef<'a>: PartialEq<T::ScalarRef<'b>>,
    {
        self.match_validity_with(counts, validity, |row, row_ptr| unsafe {
            let value = T::index_column_unchecked(col, row.to_usize());
            let scalar = row_ptr.read::<T::Scalar>(col_offset);
            let scalar = T::to_scalar_ref(&scalar);
            scalar == value
        })
    }

    fn match_with<F>(
        &mut self,
        (count, mut no_match_count): (usize, usize),
        compare_fn: F,
    ) -> (usize, usize)
    where
        F: Fn(&RowID, &RowPtr) -> bool,
    {
        assume(count <= self.compare.len());
        let mut matched = 0;
        for i in 0..count {
            let row = &self.compare[i];
            if compare_fn(row, &self.address[*row]) {
                if i != matched {
                    self.compare[matched] = *row;
                }
                matched += 1;
            } else {
                self.no_matched[no_match_count] = *row;
                no_match_count += 1;
            }
        }
        (matched, no_match_count)
    }

    fn match_validity_with<F>(
        &mut self,
        counts: (usize, usize),
        validity: Option<(&Bitmap, usize)>,
        compare_fn: F,
    ) -> (usize, usize)
    where
        F: Fn(&RowID, &RowPtr) -> bool,
    {
        if let Some((validity, offset)) = validity {
            self.match_with(counts, |row, row_ptr| {
                let a = unsafe { validity.get_bit_unchecked(row.to_usize()) };
                let b = unsafe { row_ptr.read_bool(offset) };
                match (a, b) {
                    (true, true) => compare_fn(row, row_ptr),
                    (false, false) => true,
                    _ => false,
                }
            })
        } else {
            self.match_with(counts, compare_fn)
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_column::binary::BinaryColumnBuilder;
    use databend_common_io::deserialize_bitmap;
    use databend_common_io::HybridBitmap;
    use roaring::RoaringTreemap;

    use super::*;

    #[test]
    fn serialize_bitmap_rowformat_normalizes_legacy_bytes() {
        let values = [1_u64, 5, 42];

        let mut hybrid = HybridBitmap::new();
        for v in values {
            hybrid.insert(v);
        }
        let mut hybrid_bytes = Vec::new();
        hybrid.serialize_into(&mut hybrid_bytes).unwrap();

        let mut tree = RoaringTreemap::new();
        for v in values {
            tree.insert(v);
        }
        let mut legacy_bytes = Vec::new();
        tree.serialize_into(&mut legacy_bytes).unwrap();

        let mut builder =
            BinaryColumnBuilder::with_capacity(2, hybrid_bytes.len() + legacy_bytes.len());
        builder.put_slice(&hybrid_bytes);
        builder.commit_row();
        builder.put_slice(&legacy_bytes);
        builder.commit_row();
        let column = Column::Bitmap(builder.build());

        let arena = Bump::new();
        let row_size = rowformat_size(&DataType::Bitmap);

        let mut row0 = vec![0u8; row_size];
        let mut row1 = vec![0u8; row_size];
        let mut addresses = [RowPtr::null(); BATCH_SIZE];
        addresses[0] = RowPtr::new(row0.as_mut_ptr());
        addresses[1] = RowPtr::new(row1.as_mut_ptr());

        let select_vector = [RowID::from(0), RowID::from(1)];
        let mut scratch = Vec::new();
        unsafe {
            serialize_column_to_rowformat(
                &arena,
                &column,
                &select_vector,
                &mut addresses,
                0,
                &mut scratch,
            );
        }

        let bytes0 = unsafe { addresses[0].read_bytes(0) };
        let bytes1 = unsafe { addresses[1].read_bytes(0) };

        assert_eq!(bytes0, bytes1);
        assert!(bytes0.starts_with(b"HB"));

        let decoded = deserialize_bitmap(bytes0).unwrap();
        assert_eq!(decoded.iter().collect::<Vec<_>>(), values);
    }
}

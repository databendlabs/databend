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

use std::ops::Range;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::i256;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::DecimalView;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::OpaqueType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortField;

use super::fixed_encode::fixed_encode;
use super::fixed_encode::fixed_encode_const;
use super::RowConverter;
use super::Rows;

pub fn choose_encode_method(fields: &[SortField]) -> Option<usize> {
    let mut total_len = 0;
    for field in fields {
        match field.data_type.remove_nullable() {
            DataType::Null => {}
            DataType::Boolean => total_len += bool::ENCODED_LEN,
            DataType::Number(number_data_type) => {
                with_number_mapped_type!(|NUM_TYPE| match number_data_type {
                    NumberDataType::NUM_TYPE => total_len += NUM_TYPE::ENCODED_LEN,
                });
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => total_len += T::ENCODED_LEN,
                });
            }
            DataType::Timestamp => total_len += i64::ENCODED_LEN,
            DataType::Date => total_len += i32::ENCODED_LEN,
            DataType::Interval => total_len += months_days_micros::ENCODED_LEN,
            _ => return None,
        }
    }
    Some(total_len)
}

#[derive(Debug, Clone)]
pub struct FixedRows<const N: usize>(Buffer<[u64; N]>);

impl<const N: usize> Rows for FixedRows<N> {
    const IS_ASC_COLUMN: bool = true;

    type Item<'a>
        = &'a [u64; N]
    where Self: 'a;

    type Type = OpaqueType<N>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        unsafe { self.0.get_unchecked(index) }
    }

    fn to_column(&self) -> Column {
        OpaqueType::upcast_column(self.0.clone())
    }

    fn try_from_column(col: &Column) -> Option<Self> {
        OpaqueType::<N>::try_downcast_column(col).map(FixedRows)
    }

    fn slice(&self, range: Range<usize>) -> Self {
        FixedRows(self.0.clone().sliced(range.start, range.len()))
    }

    fn scalar_as_item<'a>(s: &'a Scalar) -> Self::Item<'a> {
        OpaqueType::<N>::try_downcast_scalar(&s.as_ref()).unwrap()
    }

    fn owned_item(item: Self::Item<'_>) -> Scalar {
        OpaqueType::<N>::upcast_scalar(*item)
    }
}

impl<const N: usize> RowConverter<FixedRows<N>> for FixedRowConverter<N> {
    fn create(sort_desc: &[SortColumnDescription], output_schema: DataSchemaRef) -> Result<Self> {
        let sort_fields = sort_desc
            .iter()
            .map(|d| {
                let data_type = output_schema.field(d.offset).data_type();
                SortField::new_with_options(data_type.clone(), d.asc, d.nulls_first)
            })
            .collect::<Vec<_>>();
        FixedRowConverter::new(sort_fields)
    }

    fn convert(&self, columns: &[BlockEntry], num_rows: usize) -> Result<FixedRows<N>> {
        Ok(self.convert_columns(columns, num_rows))
    }

    fn support_data_type(d: &DataType) -> bool {
        match d {
            DataType::Null
            | DataType::Boolean
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Interval
            | DataType::Date => true,
            DataType::Nullable(inner) => Self::support_data_type(inner.as_ref()),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct FixedRowConverter<const N: usize> {
    fields: Box<[SortField]>,
}

impl<const N: usize> FixedRowConverter<N> {
    fn new(fields: Vec<SortField>) -> Result<Self> {
        if !fields.iter().all(|f| Self::support_data_type(&f.data_type)) {
            return Err(ErrorCode::Unimplemented(format!(
                "Row format is not yet support for {:?}",
                fields
            )));
        }

        Ok(Self {
            fields: fields.into(),
        })
    }

    /// Convert columns into fixed-size row format.
    fn convert_columns(&self, entries: &[BlockEntry], num_rows: usize) -> FixedRows<N> {
        debug_assert_eq!(entries.len(), self.fields.len());
        debug_assert!(entries
            .iter()
            .zip(self.fields.iter())
            .all(|(entry, f)| entry.len() == num_rows && entry.data_type() == f.data_type));

        let (mut buffer, mut offsets) = self.new_empty_rows(num_rows);
        for (entry, field) in entries.iter().zip(self.fields.iter()) {
            match entry {
                BlockEntry::Const(scalar, data_type, _) => {
                    let mut visitor = EncodeVisitor {
                        buffer: &mut buffer,
                        offsets: &mut offsets,
                        validity: (scalar.is_null(), None),
                        field,
                    };
                    visitor.visit_const_column(scalar, data_type).unwrap();
                }
                BlockEntry::Column(column) => {
                    let validity = column.validity();
                    let mut visitor = EncodeVisitor {
                        buffer: &mut buffer,
                        offsets: &mut offsets,
                        validity,
                        field,
                    };
                    visitor.visit_column(column.remove_nullable()).unwrap();
                }
            }
        }

        #[cfg(target_endian = "little")]
        {
            for arr in buffer.iter_mut() {
                for v in arr {
                    *v = v.swap_bytes();
                }
            }
        }

        FixedRows(OpaqueType::<N>::build_column(buffer))
    }

    fn new_empty_rows(&self, num_rows: usize) -> (Vec<[u64; N]>, Vec<u64>) {
        let buffer = vec![[0_u64; N]; num_rows];
        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0);

        let mut cur_offset = 0_u64;
        for _ in 0..num_rows {
            offsets.push(cur_offset);
            cur_offset = cur_offset.checked_add(N as u64 * 8).expect("overflow");
        }
        (buffer, offsets)
    }
}

struct EncodeVisitor<'a, const N: usize> {
    buffer: &'a mut Vec<[u64; N]>,
    offsets: &'a mut Vec<u64>,
    validity: (bool, Option<&'a Bitmap>),
    field: &'a SortField,
}

fn buffer_to_bytes<const N: usize>(buffer: &mut Vec<[u64; N]>) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buffer.len() * N * 8) }
}

impl<const N: usize> EncodeVisitor<'_, N> {
    fn visit_const_column(&mut self, scalar: &Scalar, data_type: &DataType) -> Result<()> {
        let is_null = scalar.is_null();
        debug_assert!(data_type.is_nullable_or_null() || !is_null);
        match data_type.remove_nullable() {
            DataType::Null => {}
            DataType::Boolean => {
                let scalar = if is_null {
                    false
                } else {
                    *scalar.as_boolean().unwrap()
                };
                let buffer_bytes = buffer_to_bytes(self.buffer);
                fixed_encode_const::<BooleanType>(
                    buffer_bytes,
                    self.offsets,
                    is_null,
                    scalar,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            DataType::Number(number_data_type) => {
                with_number_mapped_type!(|NUM_TYPE| match number_data_type {
                    NumberDataType::NUM_TYPE => {
                        let scalar_value = if is_null {
                            NUM_TYPE::default()
                        } else {
                            NumberType::<NUM_TYPE>::try_downcast_scalar(&scalar.as_ref()).unwrap()
                        };
                        let buffer_bytes = buffer_to_bytes(self.buffer);
                        fixed_encode_const::<NumberType<NUM_TYPE>>(
                            buffer_bytes,
                            self.offsets,
                            is_null,
                            scalar_value,
                            self.field.asc,
                            self.field.nulls_first,
                        );
                    }
                });
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => {
                        let scalar_value = if is_null {
                            T::default()
                        } else {
                            DecimalType::<T>::try_downcast_scalar(&scalar.as_ref()).unwrap()
                        };
                        let buffer_bytes = buffer_to_bytes(self.buffer);
                        fixed_encode_const::<DecimalType<T>>(
                            buffer_bytes,
                            self.offsets,
                            is_null,
                            scalar_value,
                            self.field.asc,
                            self.field.nulls_first,
                        );
                    }
                });
            }
            DataType::Timestamp => {
                let scalar_value = if is_null {
                    0i64
                } else {
                    *scalar.as_timestamp().unwrap()
                };
                let buffer_bytes = buffer_to_bytes(self.buffer);
                fixed_encode_const::<TimestampType>(
                    buffer_bytes,
                    self.offsets,
                    is_null,
                    scalar_value,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            DataType::Date => {
                let scalar_value = if is_null {
                    0i32
                } else {
                    *scalar.as_date().unwrap()
                };
                let buffer_bytes = buffer_to_bytes(self.buffer);
                fixed_encode_const::<DateType>(
                    buffer_bytes,
                    self.offsets,
                    is_null,
                    scalar_value,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            DataType::Interval => {
                let scalar_value = if is_null {
                    months_days_micros::default()
                } else {
                    *scalar.as_interval().unwrap()
                };
                let buffer_bytes = buffer_to_bytes(self.buffer);
                fixed_encode_const::<IntervalType>(
                    buffer_bytes,
                    self.offsets,
                    is_null,
                    scalar_value,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported data type for constant encoding: {:?}",
                    data_type
                )));
            }
        }
        Ok(())
    }
}

impl<const N: usize> ValueVisitor for EncodeVisitor<'_, N> {
    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<()> {
        unreachable!("encode_column should only be called with columns")
    }

    fn visit_null(&mut self, _len: usize) -> Result<()> {
        // Null columns don't need encoding
        Ok(())
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<()> {
        let buffer_bytes = buffer_to_bytes(self.buffer);
        fixed_encode(
            buffer_bytes,
            self.offsets,
            bitmap,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<()> {
        with_number_mapped_type!(|NUM_TYPE| match column.data_type() {
            NumberDataType::NUM_TYPE => {
                let c =
                    NumberType::<NUM_TYPE>::try_downcast_column(&Column::Number(column.clone()))
                        .unwrap();
                let buffer_bytes = buffer_to_bytes(self.buffer);
                fixed_encode(
                    buffer_bytes,
                    self.offsets,
                    c,
                    self.validity,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
        });
        Ok(())
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<()> {
        with_decimal_mapped_type!(|F| match column {
            DecimalColumn::F(buffer, size) => {
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => {
                        let iter = DecimalView::<F, T>::iter_column(&buffer);
                        let buffer_bytes = buffer_to_bytes(self.buffer);
                        fixed_encode(
                            buffer_bytes,
                            self.offsets,
                            iter,
                            self.validity,
                            self.field.asc,
                            self.field.nulls_first,
                        );
                    }
                });
            }
        });
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        let buffer_bytes = buffer_to_bytes(self.buffer);
        fixed_encode(
            buffer_bytes,
            self.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        let buffer_bytes = buffer_to_bytes(self.buffer);
        fixed_encode(
            buffer_bytes,
            self.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_interval(&mut self, buffer: Buffer<months_days_micros>) -> Result<()> {
        let buffer_bytes = buffer_to_bytes(self.buffer);
        fixed_encode(
            buffer_bytes,
            self.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        _column: T::Column,
        _data_type: &DataType,
    ) -> Result<()> {
        unimplemented!("Unsupported column type for encoding")
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::SortField;
    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::super::test_util::*;
    use super::*;

    #[test]
    fn fixed_fuzz_test() {
        let mut runner = TestRunner::default();
        for _ in 0..100 {
            let TestCase {
                entries,
                sort_options,
                num_rows,
            } = fixed_test_case_strategy()
                .new_tree(&mut runner)
                .unwrap()
                .current();

            let comparator = create_arrow_comparator(&entries, &sort_options);
            let fields = create_sort_fields(&entries, &sort_options);

            let length = choose_encode_method(&fields).unwrap();
            match length.div_ceil(8) {
                1 => test_fixed_converter::<1>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                2 => test_fixed_converter::<2>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                3 => test_fixed_converter::<3>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                4 => test_fixed_converter::<4>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                5 => test_fixed_converter::<5>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                6 => test_fixed_converter::<6>(
                    &entries,
                    &fields,
                    num_rows,
                    &comparator,
                    &sort_options,
                ),
                _ => continue,
            }
        }
    }

    pub fn fixed_column_strategy(
        len: usize,
        valid_percent: f64,
    ) -> impl Strategy<Value = BlockEntry> {
        prop_oneof![
            number_column_strategy::<i32>(len, valid_percent),
            number_column_strategy::<u32>(len, valid_percent),
            number_column_strategy::<i64>(len, valid_percent),
            number_column_strategy::<u64>(len, valid_percent),
            f32_column_strategy(len, valid_percent),
            f64_column_strategy(len, valid_percent),
        ]
    }

    pub fn fixed_test_case_strategy() -> impl Strategy<Value = TestCase> {
        let sort_options_strategy = prop::collection::vec((any::<bool>(), any::<bool>()), 1..5);
        let num_rows_strategy = 5..100_usize;

        (sort_options_strategy, num_rows_strategy)
            .prop_flat_map(|(sort_options, num_rows)| {
                (
                    prop::collection::vec(fixed_column_strategy(num_rows, 0.8), sort_options.len()),
                    Just(sort_options),
                    Just(num_rows),
                )
            })
            .prop_map(|(entries, sort_options, num_rows)| TestCase {
                entries,
                sort_options,
                num_rows,
            })
    }

    fn test_fixed_converter<const N: usize>(
        entries: &[BlockEntry],
        fields: &[SortField],
        num_rows: usize,
        comparator: &arrow_ord::sort::LexicographicalComparator,
        sort_options: &[(bool, bool)],
    ) {
        let converter = FixedRowConverter::<N>::new(fields.to_vec()).unwrap();
        let rows = converter.convert_columns(entries, num_rows);

        for i in 0..num_rows {
            for j in 0..num_rows {
                let row_i = rows.row(i);
                let row_j = rows.row(j);
                let row_cmp = row_i.cmp(row_j);
                let lex_cmp = comparator.compare(i, j);
                assert_eq!(
                    row_cmp,
                    lex_cmp,
                    "\ndata: ({:?} vs {:?})\nrow format: ({:?} vs {:?})\noptions: {:?}",
                    print_row(entries, i),
                    print_row(entries, j),
                    row_i,
                    row_j,
                    print_options(sort_options)
                );
            }
        }
    }
}

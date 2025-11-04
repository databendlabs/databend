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
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
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
use databend_common_expression::types::StringColumn;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_number_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortField;
use jsonb::RawJsonb;

use super::fixed_encode::fixed_encode;
use super::fixed_encode::fixed_encode_const;
use super::variable_encode::encoded_len;
use super::variable_encode::var_encode;
use super::RowConverter;
use super::Rows;

pub type VariableRows = BinaryColumn;

impl Rows for VariableRows {
    const IS_ASC_COLUMN: bool = true;
    type Item<'a> = &'a [u8];
    type Type = BinaryType;

    fn len(&self) -> usize {
        self.len()
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        unsafe { self.index_unchecked(index) }
    }

    fn to_column(&self) -> Column {
        Column::Binary(self.clone())
    }

    fn from_column(col: &Column) -> Result<Self> {
        match col.as_binary().cloned() {
            Some(col) => Ok(col),
            None => Err(ErrorCode::BadDataValueType(format!(
                "Order column type mismatched. Expected {} but got {}",
                Self::data_type(),
                col.data_type()
            ))),
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        self.slice(range)
    }

    fn scalar_as_item<'a>(s: &'a Scalar) -> Self::Item<'a> {
        match s {
            Scalar::Binary(s) => s,
            _ => unreachable!(),
        }
    }

    fn owned_item(item: Self::Item<'_>) -> Scalar {
        Scalar::Binary(Vec::from(item))
    }
}

impl RowConverter<VariableRows> for VariableRowConverter {
    fn create(sort_desc: &[SortColumnDescription], output_schema: DataSchemaRef) -> Result<Self> {
        let sort_fields = sort_desc
            .iter()
            .map(|d| {
                let data_type = output_schema.field(d.offset).data_type();
                SortField::new_with_options(data_type.clone(), d.asc, d.nulls_first)
            })
            .collect::<Vec<_>>();
        VariableRowConverter::new(sort_fields)
    }

    fn convert(&self, entries: &[BlockEntry], num_rows: usize) -> Result<BinaryColumn> {
        Ok(self.convert_columns(entries, num_rows))
    }

    fn support_data_type(d: &DataType) -> bool {
        match d {
            DataType::Null
            | DataType::Boolean
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Interval
            | DataType::Date
            | DataType::Binary
            | DataType::String
            | DataType::Variant => true,
            DataType::Nullable(inner) => Self::support_data_type(inner.as_ref()),
            _ => false,
        }
    }
}

/// Convert column-oriented data into comparable row-oriented data.
#[derive(Debug)]
pub struct VariableRowConverter {
    fields: Box<[SortField]>,
}

impl VariableRowConverter {
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

    /// Convert columns into [`BinaryColumn`] represented comparable row format.
    fn convert_columns(&self, entries: &[BlockEntry], num_rows: usize) -> BinaryColumn {
        debug_assert_eq!(entries.len(), self.fields.len());
        debug_assert!(entries
            .iter()
            .zip(self.fields.iter())
            .all(|(entry, f)| entry.len() == num_rows && entry.data_type() == f.data_type));

        let entries: Vec<BlockEntry> = entries
            .iter()
            .map(|entry| self.convert_json_to_comparable(entry))
            .collect();

        let mut builder = self.new_empty_rows(&entries, num_rows);
        for (entry, field) in entries.iter().zip(self.fields.iter()) {
            match entry {
                BlockEntry::Const(scalar, data_type, _) => {
                    let mut visitor = EncodeVisitor {
                        out: &mut builder,
                        validity: (scalar.is_null(), None),
                        field,
                    };
                    visitor.visit_const_column(scalar, data_type).unwrap();
                }
                BlockEntry::Column(column) => {
                    let validity = column.validity();
                    let mut visitor = EncodeVisitor {
                        out: &mut builder,
                        validity,
                        field,
                    };
                    visitor.visit_column(column.remove_nullable()).unwrap();
                }
            }
        }

        builder.build()
    }

    fn convert_json_to_comparable(&self, entry: &BlockEntry) -> BlockEntry {
        match entry {
            BlockEntry::Const(Scalar::Variant(val), data_type, num_rows) => {
                // convert variant value to comparable format.
                let raw_jsonb = RawJsonb::new(val);
                let buf = raw_jsonb.convert_to_comparable();
                BlockEntry::Const(Scalar::Variant(buf), data_type.clone(), *num_rows)
            }
            BlockEntry::Const(_, _, _) => entry.clone(),

            BlockEntry::Column(c) => {
                let data_type = c.data_type();
                if !data_type.remove_nullable().is_variant() {
                    return BlockEntry::Column(c.clone());
                }

                // convert variant value to comparable format.
                let (_, validity) = c.validity();
                let col = c.remove_nullable();
                let col = col.as_variant().unwrap();
                let mut builder =
                    BinaryColumnBuilder::with_capacity(col.len(), col.total_bytes_len());
                for (i, val) in col.iter().enumerate() {
                    if let Some(validity) = validity
                        && unsafe { !validity.get_bit_unchecked(i) }
                    {
                        builder.commit_row();
                        continue;
                    }
                    let raw_jsonb = RawJsonb::new(val);
                    let buf = raw_jsonb.convert_to_comparable();
                    builder.put_slice(buf.as_ref());
                    builder.commit_row();
                }
                if data_type.is_nullable() {
                    NullableColumn::new_column(
                        Column::Variant(builder.build()),
                        validity.unwrap().clone(),
                    )
                } else {
                    Column::Variant(builder.build())
                }
                .into()
            }
        }
    }

    fn new_empty_rows(&self, entries: &[BlockEntry], num_rows: usize) -> BinaryColumnBuilder {
        let mut lengths = vec![0_usize; num_rows];

        for entry in entries {
            match entry {
                BlockEntry::Const(scalar, data_type, _) => {
                    let mut visitor = LengthCalculatorVisitor {
                        lengths: &mut lengths,
                        validity: (scalar.is_null(), None),
                    };
                    visitor.visit_const_column(scalar, data_type).unwrap();
                }
                BlockEntry::Column(column) => {
                    let validity = column.validity();
                    let column = column.remove_nullable();
                    let mut visitor = LengthCalculatorVisitor {
                        lengths: &mut lengths,
                        validity,
                    };
                    visitor.visit_column(column).unwrap();
                }
            }
        }

        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0);

        // Comments from apache/arrow-rs:
        // We initialize the offsets shifted down by one row index.
        //
        // As the rows are appended to the offsets will be incremented to match
        //
        // For example, consider the case of 3 rows of length 3, 4, and 6 respectively.
        // The offsets would be initialized to `0, 0, 3, 7`
        //
        // Writing the first row entirely would yield `0, 3, 3, 7`
        // The second, `0, 3, 7, 7`
        // The third, `0, 3, 7, 13`
        //
        // This would be the final offsets for reading
        //
        // In this way offsets tracks the position during writing whilst eventually serving
        // as identifying the offsets of the written rows
        let mut cur_offset = 0_u64;
        for l in lengths {
            offsets.push(cur_offset);
            cur_offset = cur_offset.checked_add(l as u64).expect("overflow");
        }

        let buffer = vec![0_u8; cur_offset as usize];

        BinaryColumnBuilder::from_data(buffer, offsets)
    }
}

struct LengthCalculatorVisitor<'a> {
    lengths: &'a mut [usize],
    validity: (bool, Option<&'a Bitmap>),
}

impl LengthCalculatorVisitor<'_> {
    fn visit_const_column(&mut self, scalar: &Scalar, data_type: &DataType) -> Result<()> {
        match data_type.remove_nullable() {
            DataType::Null => {}
            DataType::Boolean => {
                self.lengths
                    .iter_mut()
                    .for_each(|x| *x += bool::ENCODED_LEN);
            }
            DataType::Number(number_data_type) => {
                with_number_mapped_type!(|NUM_TYPE| match number_data_type {
                    NumberDataType::NUM_TYPE => {
                        for length in self.lengths.iter_mut() {
                            *length += NUM_TYPE::ENCODED_LEN
                        }
                    }
                });
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => {
                        for length in self.lengths.iter_mut() {
                            *length += T::ENCODED_LEN
                        }
                    }
                });
            }
            DataType::Timestamp => {
                for length in self.lengths.iter_mut() {
                    *length += i64::ENCODED_LEN
                }
            }
            DataType::Date => {
                for length in self.lengths.iter_mut() {
                    *length += i32::ENCODED_LEN
                }
            }
            DataType::Interval => {
                for length in self.lengths.iter_mut() {
                    *length += months_days_micros::ENCODED_LEN
                }
            }

            DataType::Binary => {
                let n = if scalar.is_null() {
                    encoded_len(&[], true)
                } else {
                    encoded_len(scalar.as_binary().unwrap(), false)
                };
                for length in self.lengths.iter_mut() {
                    *length += n
                }
            }
            DataType::String => {
                let n = if scalar.is_null() {
                    encoded_len(&[], true)
                } else {
                    encoded_len(scalar.as_string().unwrap().as_bytes(), false)
                };
                for length in self.lengths.iter_mut() {
                    *length += n
                }
            }
            DataType::Variant => {
                let n = if scalar.is_null() {
                    encoded_len(&[], true)
                } else {
                    encoded_len(scalar.as_variant().unwrap(), false)
                };
                for length in self.lengths.iter_mut() {
                    *length += n
                }
            }

            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported data type for constant length calculation: {:?}",
                    data_type
                )));
            }
        }
        Ok(())
    }

    fn visit_variable_length_data<I, T, F>(&mut self, iter: I, bytes_len: F) -> Result<()>
    where
        I: Iterator<Item = T>,
        F: Fn(T, bool) -> usize,
    {
        let (all_null, validity) = self.validity;
        if all_null {
            self.lengths.iter_mut().for_each(|x| *x += 1);
        } else if let Some(validity) = validity {
            iter.zip(validity.iter())
                .zip(self.lengths.iter_mut())
                .for_each(|((item, v), length)| *length += bytes_len(item, !v));
        } else {
            iter.zip(self.lengths.iter_mut())
                .for_each(|(item, length)| *length += bytes_len(item, false));
        }
        Ok(())
    }
}

impl ValueVisitor for LengthCalculatorVisitor<'_> {
    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<()> {
        unreachable!("LengthCalculatorVisitor should only be called with columns")
    }

    fn visit_null(&mut self, _len: usize) -> Result<()> {
        // Null columns don't add any length
        Ok(())
    }

    fn visit_boolean(&mut self, _bitmap: Bitmap) -> Result<()> {
        self.lengths
            .iter_mut()
            .for_each(|x| *x += bool::ENCODED_LEN);
        Ok(())
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<()> {
        with_number_mapped_type!(|NUM_TYPE| match column.data_type() {
            NumberDataType::NUM_TYPE => {
                self.lengths
                    .iter_mut()
                    .for_each(|x| *x += NUM_TYPE::ENCODED_LEN);
            }
        });
        Ok(())
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<()> {
        with_decimal_mapped_type!(|F| match column {
            DecimalColumn::F(_, size) => {
                with_decimal_mapped_type!(|T| match size.data_kind() {
                    DecimalDataKind::T => {
                        self.lengths.iter_mut().for_each(|x| *x += T::ENCODED_LEN);
                    }
                });
            }
        });
        Ok(())
    }

    fn visit_timestamp(&mut self, _buffer: Buffer<i64>) -> Result<()> {
        self.lengths.iter_mut().for_each(|x| *x += i64::ENCODED_LEN);
        Ok(())
    }

    fn visit_date(&mut self, _buffer: Buffer<i32>) -> Result<()> {
        self.lengths.iter_mut().for_each(|x| *x += i32::ENCODED_LEN);
        Ok(())
    }

    fn visit_interval(&mut self, _buffer: Buffer<months_days_micros>) -> Result<()> {
        self.lengths
            .iter_mut()
            .for_each(|x| *x += months_days_micros::ENCODED_LEN);
        Ok(())
    }

    fn visit_binary(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_variable_length_data(column.iter(), encoded_len)
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        self.visit_variable_length_data(column.iter(), |s, is_null| {
            encoded_len(s.as_bytes(), is_null)
        })
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.visit_variable_length_data(column.iter(), encoded_len)
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        _column: T::Column,
        _data_type: &DataType,
    ) -> Result<()> {
        unimplemented!("Unsupported column type for length calculation")
    }
}

struct EncodeVisitor<'a> {
    out: &'a mut BinaryColumnBuilder,
    validity: (bool, Option<&'a Bitmap>),
    field: &'a SortField,
}

impl EncodeVisitor<'_> {
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
                fixed_encode_const::<BooleanType>(
                    &mut self.out.data,
                    &mut self.out.offsets,
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
                        fixed_encode_const::<NumberType<NUM_TYPE>>(
                            &mut self.out.data,
                            &mut self.out.offsets,
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
                        fixed_encode_const::<DecimalType<T>>(
                            &mut self.out.data,
                            &mut self.out.offsets,
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
                fixed_encode_const::<TimestampType>(
                    &mut self.out.data,
                    &mut self.out.offsets,
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
                fixed_encode_const::<DateType>(
                    &mut self.out.data,
                    &mut self.out.offsets,
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
                fixed_encode_const::<IntervalType>(
                    &mut self.out.data,
                    &mut self.out.offsets,
                    is_null,
                    scalar_value,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }

            DataType::Binary => {
                let v = if is_null {
                    [].as_slice()
                } else {
                    scalar.as_binary().unwrap()
                };
                var_encode(
                    self.out,
                    (0..self.out.len()).map(|_| v),
                    self.validity,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            DataType::String => {
                let v = if is_null {
                    [].as_slice()
                } else {
                    scalar.as_string().unwrap().as_bytes()
                };
                var_encode(
                    self.out,
                    (0..self.out.len()).map(|_| v),
                    self.validity,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            DataType::Variant => {
                let v = if is_null {
                    [].as_slice()
                } else {
                    scalar.as_variant().unwrap()
                };
                var_encode(
                    self.out,
                    (0..self.out.len()).map(|_| v),
                    self.validity,
                    self.field.asc,
                    self.field.nulls_first,
                );
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported data type for constant length calculation: {:?}",
                    data_type
                )));
            }
        }
        Ok(())
    }
}

impl ValueVisitor for EncodeVisitor<'_> {
    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<()> {
        unreachable!("encode_column should only be called with columns")
    }

    fn visit_null(&mut self, _len: usize) -> Result<()> {
        // Null columns don't need encoding
        Ok(())
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<()> {
        fixed_encode(
            &mut self.out.data,
            &mut self.out.offsets,
            bitmap,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<()> {
        with_number_type!(|NUM_TYPE| match column {
            NumberColumn::NUM_TYPE(c) => {
                fixed_encode(
                    &mut self.out.data,
                    &mut self.out.offsets,
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
                        fixed_encode(
                            &mut self.out.data,
                            &mut self.out.offsets,
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
        fixed_encode(
            &mut self.out.data,
            &mut self.out.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        fixed_encode(
            &mut self.out.data,
            &mut self.out.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_interval(&mut self, buffer: Buffer<months_days_micros>) -> Result<()> {
        fixed_encode(
            &mut self.out.data,
            &mut self.out.offsets,
            buffer,
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_binary(&mut self, column: BinaryColumn) -> Result<()> {
        var_encode(
            self.out,
            column.iter(),
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        var_encode(
            self.out,
            column.iter().map(|s| s.as_bytes()),
            self.validity,
            self.field.asc,
            self.field.nulls_first,
        );
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        var_encode(
            self.out,
            column.iter(),
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
    use databend_common_base::base::OrderedFloat;
    use databend_common_expression::types::*;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::SortField;
    use jsonb::OwnedJsonb;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::super::test_util::*;
    use super::*;

    #[test]
    fn test_fixed_width() {
        let cols = [
            Int16Type::from_opt_data(vec![
                Some(1),
                Some(2),
                None,
                Some(-5),
                Some(2),
                Some(2),
                Some(0),
            ]),
            Float32Type::from_opt_data(vec![
                Some(OrderedFloat(1.3)),
                Some(OrderedFloat(2.5)),
                None,
                Some(OrderedFloat(4.)),
                Some(OrderedFloat(0.1)),
                Some(OrderedFloat(-4.)),
                Some(OrderedFloat(-0.)),
            ]),
        ];

        let converter = VariableRowConverter::new(vec![
            SortField::new(DataType::Number(NumberDataType::Int16).wrap_nullable()),
            SortField::new(DataType::Number(NumberDataType::Float32).wrap_nullable()),
        ])
        .unwrap();

        let entries: Vec<BlockEntry> = cols.iter().map(|col| col.clone().into()).collect();
        let rows = converter.convert_columns(&entries, cols[0].len());

        assert!(rows.index(3).unwrap() < rows.index(6).unwrap());
        assert!(rows.index(0).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(4).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(5).unwrap() < rows.index(4).unwrap());
    }

    #[test]
    fn test_decimal128() {
        let converter = VariableRowConverter::new(vec![SortField::new(
            DataType::Decimal(DecimalSize::new_unchecked(38, 7)).wrap_nullable(),
        )])
        .unwrap();

        let col = Decimal128Type::from_opt_data_with_size(
            vec![
                None,
                Some(i128::MIN),
                Some(-13),
                Some(46_i128),
                Some(5456_i128),
                Some(i128::MAX),
            ],
            Some(DecimalSize::new_unchecked(38, 7)),
        );

        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        for i in 0..rows.len() - 1 {
            assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
        }
    }

    #[test]
    fn test_decimal256() {
        let converter = VariableRowConverter::new(vec![SortField::new(
            DataType::Decimal(DecimalSize::new_unchecked(76, 7)).wrap_nullable(),
        )])
        .unwrap();

        let col = Decimal256Type::from_opt_data_with_size(
            vec![
                None,
                Some(i256::DECIMAL_MIN),
                Some(i256::from_words(-1, 0)),
                Some(i256::from_words(-1, i128::MAX)),
                Some(i256::from_words(0, i128::MAX)),
                Some(i256::from_words(46_i128, 0)),
                Some(i256::from_words(46_i128, 5)),
                Some(i256::DECIMAL_MAX),
            ],
            Some(DecimalSize::new_unchecked(76, 7)),
        );

        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        for i in 0..rows.len() - 1 {
            assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
        }
    }

    #[test]
    fn test_decimal_view() {
        fn run(size: DecimalSize, col: Column) {
            let converter =
                VariableRowConverter::new(vec![SortField::new(DataType::Decimal(size))]).unwrap();
            let num_rows = col.len();
            let rows = converter.convert_columns(&[col.into()], num_rows);
            for i in 0..num_rows - 1 {
                assert!(rows.index(i).unwrap() <= rows.index(i + 1).unwrap());
            }
        }

        {
            let data = vec![-100i64, 50i64, 100i64, 200i64, 300i64];

            for p in [15, 20, 40] {
                let size = DecimalSize::new_unchecked(p, 2);
                let col = Decimal64Type::from_data_with_size(&data, Some(size));
                run(size, col);
            }
        }

        {
            let data = vec![-1000i128, 500i128, 1500i128, 2000i128, 3000i128];

            for p in [15, 20, 40] {
                let size = DecimalSize::new_unchecked(p, 3);
                let col = Decimal128Type::from_data_with_size(&data, Some(size));

                run(size, col);
            }
        }

        {
            let data = vec![
                i256::from(-5000),
                i256::from(10000),
                i256::from(15000),
                i256::from(20000),
            ];

            for p in [15, 20, 40] {
                let size = DecimalSize::new_unchecked(p, 4);
                let col = Decimal256Type::from_data_with_size(&data, Some(size));

                run(size, col);
            }
        }
    }

    #[test]
    fn test_bool() {
        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::Boolean.wrap_nullable())])
                .unwrap();

        let col = BooleanType::from_opt_data(vec![None, Some(false), Some(true)]);
        let num_rows = col.len();

        let rows = converter.convert_columns(&[col.clone().into()], num_rows);

        assert!(rows.index(2).unwrap() > rows.index(1).unwrap());
        assert!(rows.index(2).unwrap() > rows.index(0).unwrap());
        assert!(rows.index(1).unwrap() > rows.index(0).unwrap());

        let converter = VariableRowConverter::new(vec![SortField::new_with_options(
            DataType::Boolean.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();

        let rows = converter.convert_columns(&[col.into()], num_rows);

        assert!(rows.index(2).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(2).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
    }

    #[test]
    fn test_null_encoding() {
        let col = Column::Null { len: 10 };
        let converter = VariableRowConverter::new(vec![SortField::new(DataType::Null)]).unwrap();
        let rows = converter.convert_columns(&[col.into()], 10);

        assert_eq!(rows.len(), 10);
        assert_eq!(rows.index(1).unwrap().len(), 0);
    }

    #[test]
    fn test_binary() {
        let col = BinaryType::from_opt_data(vec![
            Some("hello".as_bytes().to_vec()),
            Some("he".as_bytes().to_vec()),
            None,
            Some("foo".as_bytes().to_vec()),
            Some("".as_bytes().to_vec()),
        ]);

        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(2).unwrap() < rows.index(4).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(1).unwrap());

        const BLOCK_SIZE: usize = 32;

        let col = BinaryType::from_opt_data(vec![
            None,
            Some(vec![0_u8; 0]),
            Some(vec![0_u8; 6]),
            Some(vec![0_u8; BLOCK_SIZE]),
            Some(vec![0_u8; BLOCK_SIZE + 1]),
            Some(vec![1_u8; 6]),
            Some(vec![1_u8; BLOCK_SIZE]),
            Some(vec![1_u8; BLOCK_SIZE + 1]),
            Some(vec![0xFF_u8; 6]),
            Some(vec![0xFF_u8; BLOCK_SIZE]),
            Some(vec![0xFF_u8; BLOCK_SIZE + 1]),
        ]);
        let num_rows = col.len();

        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())])
                .unwrap();
        let rows = converter.convert_columns(&[col.clone().into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() < rows.index(j).unwrap(),
                    "{} < {} - {:?} < {:?}",
                    i,
                    j,
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }

        let converter = VariableRowConverter::new(vec![SortField::new_with_options(
            DataType::Binary.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() > rows.index(j).unwrap(),
                    "{} > {} - {:?} > {:?}",
                    i,
                    j,
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }
    }

    #[test]
    fn test_string() {
        let col =
            StringType::from_opt_data(vec![Some("hello"), Some("he"), None, Some("foo"), Some("")]);

        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(2).unwrap() < rows.index(4).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(1).unwrap());

        const BLOCK_SIZE: usize = 32;

        let col = StringType::from_opt_data(vec![
            None,
            Some(String::from_utf8(vec![0_u8; 0]).unwrap()),
            Some(String::from_utf8(vec![0_u8; 6]).unwrap()),
            Some(String::from_utf8(vec![0_u8; BLOCK_SIZE]).unwrap()),
            Some(String::from_utf8(vec![0_u8; BLOCK_SIZE + 1]).unwrap()),
            Some(String::from_utf8(vec![1_u8; 6]).unwrap()),
            Some(String::from_utf8(vec![1_u8; BLOCK_SIZE]).unwrap()),
            Some(String::from_utf8(vec![1_u8; BLOCK_SIZE + 1]).unwrap()),
        ]);
        let num_rows = col.len();

        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())])
                .unwrap();
        let rows = converter.convert_columns(&[col.clone().into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() < rows.index(j).unwrap(),
                    "{} < {} - {:?} < {:?}",
                    i,
                    j,
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }

        let converter = VariableRowConverter::new(vec![SortField::new_with_options(
            DataType::String.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() > rows.index(j).unwrap(),
                    "{} > {} - {:?} > {:?}",
                    i,
                    j,
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }
    }

    #[test]
    fn test_variant() {
        let values = [
            None,
            Some("false"),
            Some("true"),
            Some("-3"),
            Some("-2.1"),
            Some("1.1"),
            Some("2"),
            Some(r#""""#),
            Some(r#""abc""#),
            Some(r#""de""#),
            Some(r#"{"k1":"v1","k2":"v2"}"#),
            Some(r#"{"k1":"v2"}"#),
            Some(r#"[1,2,3]"#),
            Some(r#"["xx",11]"#),
            Some("null"),
        ]
        .into_iter()
        .map(|x| x.map(|x| x.parse::<OwnedJsonb>().unwrap().to_vec()))
        .collect();

        let col = VariantType::from_opt_data(values);

        let converter =
            VariableRowConverter::new(vec![SortField::new(DataType::Variant.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.clone().into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() < rows.index(j).unwrap(),
                    "{i}{:?} < {j}{:?}",
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }

        let converter = VariableRowConverter::new(vec![SortField::new_with_options(
            DataType::Variant.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col.into()], num_rows);

        for i in 0..rows.len() {
            for j in i + 1..rows.len() {
                assert!(
                    rows.index(i).unwrap() >= rows.index(j).unwrap(),
                    "{i}{:?} >= {j}{:?}",
                    rows.index(i).unwrap(),
                    rows.index(j).unwrap()
                );
            }
        }
    }

    pub fn variable_column_strategy(
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
            string_column_strategy(len, valid_percent),
        ]
    }

    fn variable_test_case_strategy() -> impl Strategy<Value = TestCase> {
        let sort_options_strategy = prop::collection::vec((any::<bool>(), any::<bool>()), 1..5);
        let num_rows_strategy = 5..100_usize;

        (sort_options_strategy, num_rows_strategy)
            .prop_flat_map(|(sort_options, num_rows)| {
                (
                    prop::collection::vec(
                        variable_column_strategy(num_rows, 0.8),
                        sort_options.len(),
                    ),
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

    #[test]
    fn variable_fuzz_test() {
        let mut runner = TestRunner::default();
        for _ in 0..100 {
            let TestCase {
                entries,
                sort_options,
                num_rows,
            } = variable_test_case_strategy()
                .new_tree(&mut runner)
                .unwrap()
                .current();

            let comparator = create_arrow_comparator(&entries, &sort_options);
            let fields = create_sort_fields(&entries, &sort_options);

            let converter = VariableRowConverter::new(fields).unwrap();
            let rows = converter.convert_columns(&entries, num_rows);

            for i in 0..num_rows {
                for j in 0..num_rows {
                    let row_i = rows.index(i).unwrap();
                    let row_j = rows.index(j).unwrap();
                    let row_cmp = row_i.cmp(row_j);
                    let lex_cmp = comparator.compare(i, j);
                    assert_eq!(
                        row_cmp,
                        lex_cmp,
                        "\ndata: ({:?} vs {:?})\nrow format: ({:?} vs {:?})\noptions: {:?}",
                        print_row(&entries, i),
                        print_row(&entries, j),
                        row_i,
                        row_j,
                        print_options(&sort_options)
                    );
                }
            }
        }
    }
}

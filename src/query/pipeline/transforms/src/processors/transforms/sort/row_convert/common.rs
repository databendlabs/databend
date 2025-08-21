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

use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalView;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_number_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortField;
use jsonb::RawJsonb;

use super::fixed::fixed_encode;
use super::variable::encoded_len;
use super::variable::var_encode;
use super::RowConverter;
use super::Rows;

pub type CommonRows = BinaryColumn;

impl Rows for CommonRows {
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

    fn try_from_column(col: &Column) -> Option<Self> {
        col.as_binary().cloned()
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

impl RowConverter<CommonRows> for CommonRowConverter {
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        let sort_fields = sort_columns_descriptions
            .iter()
            .map(|d| {
                let data_type = output_schema.field(d.offset).data_type();
                SortField::new_with_options(data_type.clone(), d.asc, d.nulls_first)
            })
            .collect::<Vec<_>>();
        CommonRowConverter::new(sort_fields)
    }

    fn convert(&self, columns: &[BlockEntry], num_rows: usize) -> Result<BinaryColumn> {
        let columns = columns
            .iter()
            .map(|entry| match entry {
                BlockEntry::Const(Scalar::Variant(val), _, _) => {
                    // convert variant value to comparable format.
                    let raw_jsonb = RawJsonb::new(val);
                    let buf = raw_jsonb.convert_to_comparable();
                    let s = Scalar::Variant(buf);
                    ColumnBuilder::repeat(&s.as_ref(), num_rows, &entry.data_type()).build()
                }
                BlockEntry::Const(_, _, _) => entry.to_column(),

                BlockEntry::Column(c) => {
                    let data_type = c.data_type();
                    if !data_type.remove_nullable().is_variant() {
                        return c.clone();
                    }

                    // convert variant value to comparable format.
                    let (_, validity) = c.validity();
                    let col = c.remove_nullable();
                    let col = col.as_variant().unwrap();
                    let mut builder =
                        BinaryColumnBuilder::with_capacity(col.len(), col.total_bytes_len());
                    for (i, val) in col.iter().enumerate() {
                        if let Some(validity) = validity {
                            if unsafe { !validity.get_bit_unchecked(i) } {
                                builder.commit_row();
                                continue;
                            }
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
                }
            })
            .collect::<Vec<_>>();
        Ok(self.convert_columns(&columns, num_rows))
    }
}

/// Convert column-oriented data into comparable row-oriented data.
///
/// **NOTE**: currently, Variant is treat as String.
#[derive(Debug)]
pub struct CommonRowConverter {
    fields: Box<[SortField]>,
}

impl CommonRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self> {
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

    /// Convert columns into [`BinaryColumn`] represented comparable row format.
    fn convert_columns(&self, columns: &[Column], num_rows: usize) -> BinaryColumn {
        debug_assert_eq!(columns.len(), self.fields.len());
        debug_assert!(columns
            .iter()
            .zip(self.fields.iter())
            .all(|(col, f)| col.len() == num_rows && col.data_type() == f.data_type));

        let mut builder = self.new_empty_rows(columns, num_rows);
        for (column, field) in columns.iter().zip(self.fields.iter()) {
            encode_column(&mut builder, column, field.asc, field.nulls_first);
        }

        builder.build()
    }

    fn new_empty_rows(&self, cols: &[Column], num_rows: usize) -> BinaryColumnBuilder {
        let mut lengths = vec![0_usize; num_rows];

        for (field, col) in self.fields.iter().zip(cols.iter()) {
            // Both nullable and non-nullable data will be encoded with null sentinel byte.
            let (all_null, validity) = col.validity();
            let ty = field.data_type.remove_nullable();
            match ty {
                DataType::Null => {}
                DataType::Boolean => lengths.iter_mut().for_each(|x| *x += bool::ENCODED_LEN),
                DataType::Number(t) => with_number_mapped_type!(|NUM_TYPE| match t {
                    NumberDataType::NUM_TYPE => {
                        lengths.iter_mut().for_each(|x| *x += NUM_TYPE::ENCODED_LEN)
                    }
                }),
                DataType::Decimal(size) => {
                    with_decimal_mapped_type!(|F| match size.data_kind() {
                        DecimalDataKind::F => {
                            lengths.iter_mut().for_each(|x| *x += F::ENCODED_LEN)
                        }
                    });
                }
                DataType::Timestamp => lengths.iter_mut().for_each(|x| *x += i64::ENCODED_LEN),
                DataType::Interval => lengths
                    .iter_mut()
                    .for_each(|x| *x += months_days_micros::ENCODED_LEN),
                DataType::Date => lengths.iter_mut().for_each(|x| *x += i32::ENCODED_LEN),
                DataType::Binary => {
                    let col = col.remove_nullable();
                    if all_null {
                        lengths.iter_mut().for_each(|x| *x += 1)
                    } else if let Some(validity) = validity {
                        col.as_binary()
                            .unwrap()
                            .iter()
                            .zip(validity.iter())
                            .zip(lengths.iter_mut())
                            .for_each(|((bytes, v), length)| *length += encoded_len(bytes, !v))
                    } else {
                        col.as_binary()
                            .unwrap()
                            .iter()
                            .zip(lengths.iter_mut())
                            .for_each(|(bytes, length)| *length += encoded_len(bytes, false))
                    }
                }
                DataType::String => {
                    let col = col.remove_nullable();
                    if all_null {
                        lengths.iter_mut().for_each(|x| *x += 1)
                    } else if let Some(validity) = validity {
                        col.as_string()
                            .unwrap()
                            .iter()
                            .zip(validity.iter())
                            .zip(lengths.iter_mut())
                            .for_each(|((s, v), length)| *length += encoded_len(s.as_bytes(), !v))
                    } else {
                        col.as_string()
                            .unwrap()
                            .iter()
                            .zip(lengths.iter_mut())
                            .for_each(|(s, length)| *length += encoded_len(s.as_bytes(), false))
                    }
                }
                DataType::Variant => {
                    let col = col.remove_nullable();
                    if all_null {
                        lengths.iter_mut().for_each(|x| *x += 1)
                    } else if let Some(validity) = validity {
                        col.as_variant()
                            .unwrap()
                            .iter()
                            .zip(validity.iter())
                            .zip(lengths.iter_mut())
                            .for_each(|((bytes, v), length)| *length += encoded_len(bytes, !v))
                    } else {
                        col.as_variant()
                            .unwrap()
                            .iter()
                            .zip(lengths.iter_mut())
                            .for_each(|(bytes, length)| *length += encoded_len(bytes, false))
                    }
                }
                _ => unreachable!(),
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

fn encode_column(out: &mut BinaryColumnBuilder, column: &Column, asc: bool, nulls_first: bool) {
    let validity = column.validity();
    let column = column.remove_nullable();
    match column {
        Column::Null { .. } => {}
        Column::Boolean(col) => fixed_encode(out, col, validity, asc, nulls_first),
        Column::Number(col) => {
            with_number_type!(|NUM_TYPE| match col {
                NumberColumn::NUM_TYPE(c) => {
                    fixed_encode(out, c, validity, asc, nulls_first)
                }
            })
        }
        Column::Decimal(col) => {
            with_decimal_mapped_type!(|F| match col {
                DecimalColumn::F(buffer, size) => {
                    with_decimal_mapped_type!(|T| match size.data_kind() {
                        DecimalDataKind::T => {
                            let iter = DecimalView::<F, T>::iter_column(&buffer);
                            fixed_encode(out, iter, validity, asc, nulls_first)
                        }
                    });
                }
            });
        }
        Column::Timestamp(col) => fixed_encode(out, col, validity, asc, nulls_first),
        Column::Interval(col) => fixed_encode(out, col, validity, asc, nulls_first),
        Column::Date(col) => fixed_encode(out, col, validity, asc, nulls_first),
        Column::Binary(col) => var_encode(out, col.iter(), validity, asc, nulls_first),
        Column::String(col) => var_encode(
            out,
            col.iter().map(|s| s.as_bytes()),
            validity,
            asc,
            nulls_first,
        ),
        Column::Variant(col) => var_encode(out, col.iter(), validity, asc, nulls_first),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrayRef;
    use arrow_ord::sort::LexicographicalComparator;
    use arrow_ord::sort::SortColumn;
    use arrow_schema::SortOptions;
    use databend_common_base::base::OrderedFloat;
    use databend_common_column::bitmap::MutableBitmap;
    use databend_common_expression::types::*;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::SortField;
    use itertools::Itertools;
    use jsonb::parse_owned_jsonb;
    use rand::distributions::Alphanumeric;
    use rand::distributions::Standard;
    use rand::prelude::Distribution;
    use rand::thread_rng;
    use rand::Rng;

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

        let converter = CommonRowConverter::new(vec![
            SortField::new(DataType::Number(NumberDataType::Int16).wrap_nullable()),
            SortField::new(DataType::Number(NumberDataType::Float32).wrap_nullable()),
        ])
        .unwrap();

        let rows = converter.convert_columns(&cols, cols[0].len());

        assert!(rows.index(3).unwrap() < rows.index(6).unwrap());
        assert!(rows.index(0).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(4).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(5).unwrap() < rows.index(4).unwrap());
    }

    #[test]
    fn test_decimal128() {
        let converter = CommonRowConverter::new(vec![SortField::new(
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
        let rows = converter.convert_columns(&[col], num_rows);

        for i in 0..rows.len() - 1 {
            assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
        }
    }

    #[test]
    fn test_decimal256() {
        let converter = CommonRowConverter::new(vec![SortField::new(
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
        let rows = converter.convert_columns(&[col], num_rows);

        for i in 0..rows.len() - 1 {
            assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
        }
    }

    #[test]
    fn test_decimal_view() {
        fn run(size: DecimalSize, col: Column) {
            let converter =
                CommonRowConverter::new(vec![SortField::new(DataType::Decimal(size))]).unwrap();
            let num_rows = col.len();
            let rows = converter.convert_columns(&[col], num_rows);
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
            CommonRowConverter::new(vec![SortField::new(DataType::Boolean.wrap_nullable())])
                .unwrap();

        let col = BooleanType::from_opt_data(vec![None, Some(false), Some(true)]);
        let num_rows = col.len();

        let rows = converter.convert_columns(&[col.clone()], num_rows);

        assert!(rows.index(2).unwrap() > rows.index(1).unwrap());
        assert!(rows.index(2).unwrap() > rows.index(0).unwrap());
        assert!(rows.index(1).unwrap() > rows.index(0).unwrap());

        let converter = CommonRowConverter::new(vec![SortField::new_with_options(
            DataType::Boolean.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();

        let rows = converter.convert_columns(&[col], num_rows);

        assert!(rows.index(2).unwrap() < rows.index(1).unwrap());
        assert!(rows.index(2).unwrap() < rows.index(0).unwrap());
        assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
    }

    #[test]
    fn test_null_encoding() {
        let col = Column::Null { len: 10 };
        let converter = CommonRowConverter::new(vec![SortField::new(DataType::Null)]).unwrap();
        let rows = converter.convert_columns(&[col], 10);

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
            CommonRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col], num_rows);

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
            CommonRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())])
                .unwrap();
        let rows = converter.convert_columns(&[col.clone()], num_rows);

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

        let converter = CommonRowConverter::new(vec![SortField::new_with_options(
            DataType::Binary.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col], num_rows);

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
            CommonRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col], num_rows);

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
            CommonRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())])
                .unwrap();
        let rows = converter.convert_columns(&[col.clone()], num_rows);

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

        let converter = CommonRowConverter::new(vec![SortField::new_with_options(
            DataType::String.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col], num_rows);

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
        let values = vec![
            None,
            Some("false".to_string()),
            Some("true".to_string()),
            Some("-3".to_string()),
            Some("-2.1".to_string()),
            Some("1.1".to_string()),
            Some("2".to_string()),
            Some("\"\"".to_string()),
            Some("\"abc\"".to_string()),
            Some("\"de\"".to_string()),
            Some("{\"k1\":\"v1\",\"k2\":\"v2\"}".to_string()),
            Some("{\"k1\":\"v2\"}".to_string()),
            Some("[1,2,3]".to_string()),
            Some("[\"xx\",11]".to_string()),
            Some("null".to_string()),
        ];

        let mut validity = MutableBitmap::with_capacity(values.len());
        let mut builder = BinaryColumnBuilder::with_capacity(values.len(), values.len() * 10);
        for value in values {
            if let Some(value) = value {
                validity.push(true);
                let owned_jsonb = parse_owned_jsonb(value.as_bytes()).unwrap();
                let raw_jsonb = owned_jsonb.as_raw();
                let compare_buf = raw_jsonb.convert_to_comparable();
                builder.put_slice(&compare_buf);
            } else {
                validity.push(false);
            }
            builder.commit_row();
        }
        let col = NullableColumn::new_column(Column::Variant(builder.build()), validity.into());

        let converter =
            CommonRowConverter::new(vec![SortField::new(DataType::Variant.wrap_nullable())])
                .unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col.clone()], num_rows);

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

        let converter = CommonRowConverter::new(vec![SortField::new_with_options(
            DataType::Variant.wrap_nullable(),
            false,
            false,
        )])
        .unwrap();
        let rows = converter.convert_columns(&[col], num_rows);

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

    fn print_row(cols: &[Column], row: usize) -> String {
        let t: Vec<_> = cols
            .iter()
            .map(|x| format!("{:?}", x.index(row).unwrap()))
            .collect();
        t.join(",")
    }

    fn print_options(cols: &[(bool, bool)]) -> String {
        let t: Vec<_> = cols
            .iter()
            .map(|(asc, null_first)| {
                format!(
                    "({}, {})",
                    if *asc { "ASC" } else { "DESC" },
                    if *null_first {
                        "NULL_FIRST"
                    } else {
                        "NULL_LAST"
                    }
                )
            })
            .collect();
        t.join(",")
    }

    fn generate_column(len: usize) -> Column {
        let mut rng = thread_rng();
        match rng.gen_range(0..7) {
            0 => generate_number_column::<i32>(len, 0.8),
            1 => generate_number_column::<u32>(len, 0.8),
            2 => generate_number_column::<i64>(len, 0.8),
            3 => generate_number_column::<u64>(len, 0.8),
            4 => generate_number_column::<F32>(len, 0.8),
            5 => generate_number_column::<F64>(len, 0.8),
            6 => generate_string_column(len, 0.8),
            _ => unreachable!(),
        }
    }

    fn generate_string_column(len: usize, valid_percent: f64) -> Column {
        let mut rng = thread_rng();
        let data = (0..len)
            .map(|_| {
                rng.gen_bool(valid_percent).then(|| {
                    let len = rng.gen_range(0..100);
                    thread_rng()
                        .sample_iter(&Alphanumeric)
                        // randomly generate 5 characters.
                        .take(len)
                        .map(char::from)
                        .collect::<String>()
                })
            })
            .collect::<Vec<_>>();
        StringType::from_opt_data(data)
    }

    fn generate_number_column<K>(len: usize, valid_percent: f64) -> Column
    where
        K: Number,
        Standard: Distribution<K>,
        NumberType<K>: FromData<K>,
    {
        let mut rng = thread_rng();
        let data = (0..len)
            .map(|_| rng.gen_bool(valid_percent).then(|| rng.gen()))
            .collect_vec();
        NumberType::<K>::from_opt_data(data)
    }

    #[test]
    fn fuzz_test() {
        for _ in 0..100 {
            let mut rng = thread_rng();
            let num_columns = rng.gen_range(1..5);
            let num_rows = rng.gen_range(5..100);
            let columns = (0..num_columns)
                .map(|_| generate_column(num_rows))
                .collect::<Vec<_>>();

            let options = (0..num_columns)
                .map(|_| (rng.gen_bool(0.5), rng.gen_bool(0.5)))
                .collect::<Vec<_>>();

            let order_columns = columns
                .iter()
                .map(|col| col.clone().into_arrow_rs())
                .collect::<Vec<ArrayRef>>();

            let sort_columns = options
                .iter()
                .zip(order_columns.iter())
                .map(|((asc, nulls_first), a)| SortColumn {
                    values: a.clone(),
                    options: Some(SortOptions {
                        descending: !*asc,
                        nulls_first: *nulls_first,
                    }),
                })
                .collect::<Vec<_>>();

            let comparator = LexicographicalComparator::try_new(&sort_columns).unwrap();

            let fields = options
                .iter()
                .zip(&columns)
                .map(|((asc, nulls_first), col)| {
                    SortField::new_with_options(col.data_type(), *asc, *nulls_first)
                })
                .collect();

            let converter = CommonRowConverter::new(fields).unwrap();
            let rows = converter.convert_columns(&columns, num_rows);

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
                        print_row(&columns, i),
                        print_row(&columns, j),
                        row_i,
                        row_j,
                        print_options(&options)
                    );
                }
            }
        }
    }
}

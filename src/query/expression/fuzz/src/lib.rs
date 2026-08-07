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

use arbitrary::Arbitrary;
use arbitrary::Error;
use arbitrary::Unstructured;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::date::DATE_MAX;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::i256;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;

const MAX_ROWS: usize = 8;
const MAX_DEPTH: usize = 3;
const MAX_ARRAY_LEN: usize = 8;
const MAX_STRING_LEN: usize = 50;
const MAX_BINARY_LEN: usize = 50;

#[derive(Debug)]
pub struct ColumnCase {
    column: Column,
}

struct ColumnGenerator<'a, 'u> {
    u: &'u mut Unstructured<'a>,
}

impl<'a, 'u> ColumnGenerator<'a, 'u> {
    fn new(u: &'u mut Unstructured<'a>) -> Self {
        Self { u }
    }

    fn data_type(&mut self, depth: usize, allow_nullable: bool) -> arbitrary::Result<DataType> {
        let max_tag = if depth < MAX_DEPTH { 15 } else { 11 };
        let mut tag = self.u.int_in_range(0_u8..=max_tag)?;
        if !allow_nullable && matches!(tag, 0 | 12) {
            tag = 3;
        }

        Ok(match tag {
            0 => DataType::Null,
            1 => DataType::EmptyArray,
            2 => DataType::EmptyMap,
            3 => DataType::Boolean,
            4 => DataType::Number(*self.u.choose(ALL_NUMERICS_TYPES)?),
            5 => DataType::Decimal(self.decimal_size()?),
            6 => DataType::String,
            7 => DataType::Timestamp,
            8 => DataType::TimestampTz,
            9 => DataType::Date,
            10 => DataType::Interval,
            11 => DataType::Binary,
            12 => DataType::Nullable(Box::new(self.data_type(depth + 1, false)?)),
            13 => DataType::Array(Box::new(self.data_type(depth + 1, true)?)),
            14 => DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::String,
                self.data_type(depth + 1, true)?,
            ]))),
            15 => {
                let fields = self.u.int_in_range(1_usize..=3)?;
                let fields = (0..fields)
                    .map(|_| self.data_type(depth + 1, true))
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                DataType::Tuple(fields)
            }
            _ => unreachable!(),
        })
    }

    fn column(&mut self, data_type: &DataType, rows: usize) -> arbitrary::Result<Column> {
        let mut builder = ColumnBuilder::with_capacity(data_type, rows);
        for _ in 0..rows {
            let scalar = self.scalar(data_type)?;
            if !scalar.as_ref().is_value_of_type(data_type) {
                return Err(Error::IncorrectFormat);
            }
            builder.push(scalar.as_ref());
        }
        Ok(builder.build())
    }

    fn scalar(&mut self, data_type: &DataType) -> arbitrary::Result<Scalar> {
        Ok(match data_type {
            DataType::Null => Scalar::Null,
            DataType::EmptyArray => Scalar::EmptyArray,
            DataType::EmptyMap => Scalar::EmptyMap,
            DataType::Boolean => Scalar::Boolean(bool::arbitrary(self.u)?),
            DataType::Number(data_type) => Scalar::Number(self.number(data_type)?),
            DataType::Decimal(size) => Scalar::Decimal(self.decimal(*size)?),
            DataType::String => Scalar::String(self.string()?),
            DataType::Timestamp => {
                Scalar::Timestamp(self.u.int_in_range(TIMESTAMP_MIN..=TIMESTAMP_MAX)?)
            }
            DataType::TimestampTz => {
                let timestamp = self.u.int_in_range(TIMESTAMP_MIN..=TIMESTAMP_MAX)?;
                let offset = self.u.int_in_range(-86_400_i32..=86_400_i32)?;
                Scalar::TimestampTz(timestamp_tz::new(timestamp, offset))
            }
            DataType::Date => Scalar::Date(self.u.int_in_range(DATE_MIN..=DATE_MAX)?),
            DataType::Interval => Scalar::Interval(months_days_micros::new(
                i32::arbitrary(self.u)?,
                i32::arbitrary(self.u)?,
                i64::arbitrary(self.u)?,
            )),
            DataType::Binary => Scalar::Binary(self.bytes(MAX_BINARY_LEN)?),
            DataType::Nullable(inner) => {
                if bool::arbitrary(self.u)? {
                    Scalar::Null
                } else {
                    self.scalar(inner)?
                }
            }
            DataType::Array(inner) => {
                let rows = self.u.int_in_range(0_usize..=MAX_ARRAY_LEN)?;
                Scalar::Array(self.column(inner, rows)?)
            }
            DataType::Map(inner) => {
                let rows = self.u.int_in_range(0_usize..=MAX_ARRAY_LEN)?;
                Scalar::Map(self.column(inner, rows)?)
            }
            DataType::Tuple(fields) => Scalar::Tuple(
                fields
                    .iter()
                    .map(|field| self.scalar(field))
                    .collect::<arbitrary::Result<Vec<_>>>()?,
            ),
            _ => return Err(Error::IncorrectFormat),
        })
    }

    fn number(&mut self, data_type: &NumberDataType) -> arbitrary::Result<NumberScalar> {
        Ok(match data_type {
            NumberDataType::UInt8 => NumberScalar::UInt8(u8::arbitrary(self.u)?),
            NumberDataType::UInt16 => NumberScalar::UInt16(u16::arbitrary(self.u)?),
            NumberDataType::UInt32 => NumberScalar::UInt32(u32::arbitrary(self.u)?),
            NumberDataType::UInt64 => NumberScalar::UInt64(u64::arbitrary(self.u)?),
            NumberDataType::Int8 => NumberScalar::Int8(i8::arbitrary(self.u)?),
            NumberDataType::Int16 => NumberScalar::Int16(i16::arbitrary(self.u)?),
            NumberDataType::Int32 => NumberScalar::Int32(i32::arbitrary(self.u)?),
            NumberDataType::Int64 => NumberScalar::Int64(i64::arbitrary(self.u)?),
            NumberDataType::Float32 => NumberScalar::Float32(f32::arbitrary(self.u)?.into()),
            NumberDataType::Float64 => NumberScalar::Float64(f64::arbitrary(self.u)?.into()),
        })
    }

    fn decimal_size(&mut self) -> arbitrary::Result<DecimalSize> {
        let precision = self.u.int_in_range(1_u8..=76)?;
        let scale = self.u.int_in_range(0_u8..=precision)?;
        Ok(DecimalSize::new(precision, scale).unwrap())
    }

    fn decimal(&mut self, size: DecimalSize) -> arbitrary::Result<DecimalScalar> {
        let precision = size.precision();
        Ok(match precision {
            1..=18 => {
                let max = 10_i64.pow(precision as u32) - 1;
                DecimalScalar::Decimal64(self.u.int_in_range(-max..=max)?, size)
            }
            19..=38 => {
                let max = 10_i128.pow(precision as u32) - 1;
                DecimalScalar::Decimal128(self.u.int_in_range(-max..=max)?, size)
            }
            39..=76 => DecimalScalar::Decimal256(i256::from(i128::arbitrary(self.u)?), size),
            _ => unreachable!(),
        })
    }

    fn string(&mut self) -> arbitrary::Result<String> {
        let len = self.u.int_in_range(0_usize..=MAX_STRING_LEN)?;
        (0..len).map(|_| char::arbitrary(self.u)).collect()
    }

    fn bytes(&mut self, max_len: usize) -> arbitrary::Result<Vec<u8>> {
        let len = self.u.int_in_range(0_usize..=max_len)?;
        (0..len).map(|_| u8::arbitrary(self.u)).collect()
    }
}

impl<'a> Arbitrary<'a> for ColumnCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let rows = u.int_in_range(0_usize..=MAX_ROWS)?;
        let mut generator = ColumnGenerator::new(u);
        let data_type = generator.data_type(0, true)?;
        let column = generator.column(&data_type, rows)?;
        column.check_valid().map_err(|_| Error::IncorrectFormat)?;
        Ok(Self { column })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (2, None)
    }
}

/// Decode and run the same typed input used by libFuzzer from raw bytes.
pub fn run_fuzz_bytes(data: &[u8]) {
    let unstructured = Unstructured::new(data);
    if let Ok(case) = ColumnCase::arbitrary_take_rest(unstructured) {
        run_column_case(case);
    }
}

/// Assert that every scalar physically present in the column belongs to its domain.
pub fn run_column_case(case: ColumnCase) {
    assert_column_domain(&case.column);
}

fn assert_column_domain(column: &Column) {
    let data_type = column.data_type();
    let domain = column.domain();
    assert!(
        domain.matches_data_type(&data_type),
        "column domain has the wrong type: type={data_type}, domain={domain:?}, column={column:?}"
    );

    for (index, value) in column.iter().enumerate() {
        let value_domain = value.domain(&data_type);
        assert!(
            value_belongs_to_domain(&value, &data_type, &domain),
            "column value escaped its domain: index={index}, type={data_type}, value={value:?}, \
             domain={domain:?}, value_domain={value_domain:?}, column={column:?}"
        );
    }
}

fn value_belongs_to_domain(value: &ScalarRef<'_>, data_type: &DataType, domain: &Domain) -> bool {
    match (value, data_type, domain) {
        (ScalarRef::Null, DataType::Null, Domain::Nullable(domain)) => domain.has_null,
        (ScalarRef::Null, DataType::Nullable(_), Domain::Nullable(domain)) => domain.has_null,
        (value, DataType::Nullable(data_type), Domain::Nullable(domain)) => domain
            .value
            .as_deref()
            .is_some_and(|domain| value_belongs_to_domain(value, data_type, domain)),
        (ScalarRef::EmptyArray, DataType::EmptyArray, Domain::Array(None)) => true,
        (ScalarRef::EmptyMap, DataType::EmptyMap, Domain::Map(None)) => true,
        (ScalarRef::Array(values), DataType::Array(data_type), Domain::Array(domain)) => {
            match domain {
                None => values.len() == 0,
                Some(domain) => values
                    .iter()
                    .all(|value| value_belongs_to_domain(&value, data_type, domain)),
            }
        }
        (ScalarRef::Map(values), DataType::Map(data_type), Domain::Map(domain)) => match domain {
            None => values.len() == 0,
            Some(domain) => values
                .iter()
                .all(|value| value_belongs_to_domain(&value, data_type, domain)),
        },
        (ScalarRef::Tuple(values), DataType::Tuple(data_types), Domain::Tuple(domains)) => {
            values.len() == data_types.len()
                && values.len() == domains.len()
                && values
                    .iter()
                    .zip(data_types)
                    .zip(domains)
                    .all(|((value, data_type), domain)| {
                        value_belongs_to_domain(value, data_type, domain)
                    })
        }
        (
            ScalarRef::Binary(_)
            | ScalarRef::Bitmap(_)
            | ScalarRef::Variant(_)
            | ScalarRef::Geometry(_)
            | ScalarRef::Geography(_)
            | ScalarRef::Vector(_)
            | ScalarRef::Opaque(_),
            _,
            Domain::Undefined,
        ) => true,
        _ => primitive_domain_contains(domain, &value.domain(data_type)),
    }
}

fn primitive_domain_contains(domain: &Domain, other: &Domain) -> bool {
    match (domain, other) {
        (Domain::Number(domain), Domain::Number(other)) => {
            let mut merged = *domain;
            merged.merge(other).is_ok() && merged == *domain
        }
        (Domain::Decimal(domain), Domain::Decimal(other)) => {
            let mut merged = *domain;
            merged.merge(other).is_ok() && merged == *domain
        }
        (Domain::Boolean(domain), Domain::Boolean(other)) => {
            (!other.has_false || domain.has_false) && (!other.has_true || domain.has_true)
        }
        (Domain::String(domain), Domain::String(other)) => {
            domain.min <= other.min
                && match (&domain.max, &other.max) {
                    (None, _) => true,
                    (Some(_), None) => false,
                    (Some(domain), Some(other)) => other <= domain,
                }
        }
        (Domain::Timestamp(domain), Domain::Timestamp(other)) => {
            domain.min <= other.min && other.max <= domain.max
        }
        (Domain::TimestampTz(domain), Domain::TimestampTz(other)) => {
            domain.min <= other.min && other.max <= domain.max
        }
        (Domain::Date(domain), Domain::Date(other)) => {
            domain.min <= other.min && other.max <= domain.max
        }
        (Domain::Interval(domain), Domain::Interval(other)) => {
            domain.min <= other.min && other.max <= domain.max
        }
        (Domain::Undefined, Domain::Undefined) => true,
        _ => false,
    }
}

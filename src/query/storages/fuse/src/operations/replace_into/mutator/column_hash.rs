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

use std::hash::Hash;
use std::hash::Hasher;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use siphasher::sip128;
use siphasher::sip128::Hasher128;

pub(crate) trait RowScalarValue {
    fn row_scalar(&self, idx: usize) -> Result<ScalarRef<'_>>;
}

impl RowScalarValue for Value<AnyType> {
    fn row_scalar(&self, idx: usize) -> Result<ScalarRef<'_>> {
        match self {
            Value::Scalar(v) => Ok(v.as_ref()),
            Value::Column(c) => c.index(idx).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "index out of range while getting row scalar value from column. idx {}, len {}",
                    idx,
                    c.len()
                ))
            }),
        }
    }
}

#[inline]
fn write_bytes_with_len(hasher: &mut impl Hasher, bytes: &[u8]) {
    hasher.write_u64(bytes.len() as u64);
    hasher.write(bytes);
}

/// For row contains null value, None will be returned
pub fn row_hash_of_columns(
    column_values: &[&Value<AnyType>],
    row_idx: usize,
) -> Result<Option<u128>> {
    let mut sip = sip128::SipHasher24::new();
    for col in column_values {
        let value = col.row_scalar(row_idx)?;
        match value {
            ScalarRef::Null => {
                // the whole row is ignored if any column is null
                return Ok(None);
            }
            ScalarRef::Number(v) => match v {
                NumberScalar::UInt8(v) => sip.write_u8(v),
                NumberScalar::UInt16(v) => sip.write_u16(v),
                NumberScalar::UInt32(v) => sip.write_u32(v),
                NumberScalar::UInt64(v) => sip.write_u64(v),
                NumberScalar::Int8(v) => sip.write_i8(v),
                NumberScalar::Int16(v) => sip.write_i16(v),
                NumberScalar::Int32(v) => sip.write_i32(v),
                NumberScalar::Int64(v) => sip.write_i64(v),
                NumberScalar::Float32(v) => sip.write_u32(v.canonicalize().to_bits()),
                NumberScalar::Float64(v) => sip.write_u64(v.canonicalize().to_bits()),
            },
            ScalarRef::Timestamp(v) => sip.write_i64(v),
            ScalarRef::String(v) => write_bytes_with_len(&mut sip, v.as_bytes()),
            ScalarRef::Bitmap(v) => write_bytes_with_len(&mut sip, v),
            ScalarRef::Decimal(v) => match v {
                DecimalScalar::Decimal64(i, size) => {
                    sip.write_i64(i);
                    sip.write_u8(size.precision());
                    sip.write_u8(size.scale())
                }
                DecimalScalar::Decimal128(i, size) => {
                    sip.write_i128(i);
                    sip.write_u8(size.precision());
                    sip.write_u8(size.scale())
                }
                DecimalScalar::Decimal256(i, size) => {
                    let le_bytes = i.to_le_bytes();
                    sip.write(&le_bytes);
                    sip.write_u8(size.precision());
                    sip.write_u8(size.scale())
                }
            },
            ScalarRef::Boolean(v) => sip.write_u8(v as u8),
            ScalarRef::Date(d) => sip.write_i32(d),
            // `ScalarRef::hash` canonicalizes nested floats and keeps NULL
            // positions and container boundaries distinguishable.
            _ => value.hash(&mut sip),
        }
    }
    Ok(Some(sip.finish128().as_u128()))
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::Float32Type;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt8Type;
    use databend_common_expression::types::VectorColumn;

    use super::*;

    /// Rows 0 and 1 hold two members of one float equality class, row 2 an
    /// unrelated value. Both the direct number branch and nested containers
    /// must derive the same digest for rows 0 and 1.
    #[test]
    fn test_float_equality_class_row_hash() -> Result<()> {
        let classes: [(Vec<f32>, Vec<f64>); 2] = [
            (vec![-0.0, 0.0, 1.0], vec![-0.0, 0.0, 1.0]),
            (vec![f32::NAN, f32::from_bits(0xffc0_0001), 0.0], vec![
                f64::NAN,
                f64::from_bits(0xfff8_0000_0000_0001),
                0.0,
            ]),
        ];
        for (f32s, f64s) in classes {
            let f32_col = Float32Type::from_data(f32s.clone());
            let f64_col = Float64Type::from_data(f64s);
            let vector = Column::Vector(VectorColumn::Float32((
                f32s.into_iter().map(F32::from).collect::<Vec<_>>().into(),
                1,
            )));
            let array = Column::Array(Box::new(ArrayColumn::new(
                f64_col.clone(),
                vec![0, 1, 2, 3].into(),
            )));
            let tuple = Column::Tuple(vec![f32_col.clone(), f64_col.clone()]);
            for column in [f32_col, f64_col, vector, array, tuple] {
                let column = Value::Column(column);
                let hashes = (0..3)
                    .map(|row| row_hash_of_columns(&[&column], row))
                    .collect::<Result<Vec<_>>>()?;
                assert_eq!(hashes[0], hashes[1], "{column:?}");
                assert_ne!(hashes[0], hashes[2], "{column:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_nested_null_row_hashes() -> Result<()> {
        let columns = [
            UInt8Type::from_data_with_validity(vec![0, 1], vec![false, true]),
            UInt8Type::from_data_with_validity(vec![1, 0], vec![true, false]),
            UInt8Type::from_data_with_validity(vec![0, 1], vec![true, true]),
        ];
        let wrappers: [fn(Column) -> Scalar; 3] = [
            |column| Scalar::Tuple(vec![Scalar::Array(column)]),
            |column| {
                Scalar::Tuple(vec![Scalar::Tuple(
                    column.iter().map(|value| value.to_owned()).collect(),
                )])
            },
            |column| {
                Scalar::Map(Column::Tuple(vec![
                    StringType::from_data(vec!["a", "b"]),
                    column,
                ]))
            },
        ];
        for wrap in wrappers {
            let hashes = columns
                .iter()
                .map(|column| row_hash_of_columns(&[&Value::Scalar(wrap(column.clone()))], 0))
                .collect::<Result<Vec<_>>>()?;
            assert_ne!(hashes[0], hashes[1]);
            assert_ne!(hashes[0], hashes[2]);
            assert_ne!(hashes[1], hashes[2]);
        }
        Ok(())
    }
}

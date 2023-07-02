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

use std::hash::Hasher;

use common_expression::types::decimal::DecimalScalar;
use common_expression::types::DecimalSize;
use common_expression::types::NumberScalar;
use common_expression::Column;
use common_expression::ScalarRef;
use siphasher::sip128;
use siphasher::sip128::Hasher128;

pub fn row_hash_of_columns(columns: &[&Column], row_idx: usize) -> u128 {
    let mut sip = sip128::SipHasher24::new();
    for column in columns {
        let value = column
            .index(row_idx)
            .expect("column index out of range (calculate columns hash)");
        match value {
            ScalarRef::Number(v) => match v {
                NumberScalar::UInt8(v) => sip.write_u8(v),
                NumberScalar::UInt16(v) => sip.write_u16(v),
                NumberScalar::UInt32(v) => sip.write_u32(v),
                NumberScalar::UInt64(v) => sip.write_u64(v),
                NumberScalar::Int8(v) => sip.write_i8(v),
                NumberScalar::Int16(v) => sip.write_i16(v),
                NumberScalar::Int32(v) => sip.write_i32(v),
                NumberScalar::Int64(v) => sip.write_i64(v),
                NumberScalar::Float32(v) => sip.write_u32(v.to_bits()),
                NumberScalar::Float64(v) => sip.write_u64(v.to_bits()),
            },
            ScalarRef::Timestamp(v) => sip.write_i64(v),
            ScalarRef::String(v) => sip.write(v),
            ScalarRef::Bitmap(v) => sip.write(v),
            ScalarRef::Decimal(v) => match v {
                DecimalScalar::Decimal128(i, DecimalSize { precision, scale }) => {
                    sip.write_i128(i);
                    sip.write_u8(precision);
                    sip.write_u8(scale)
                }
                DecimalScalar::Decimal256(i, DecimalSize { precision, scale }) => {
                    let le_bytes = i.to_le_bytes();
                    sip.write(&le_bytes);
                    sip.write_u8(precision);
                    sip.write_u8(scale)
                }
            },
            ScalarRef::Boolean(v) => sip.write_u8(v as u8),
            ScalarRef::Date(d) => sip.write_i32(d),
            _ => {
                let string = value.to_string();
                sip.write(string.as_bytes());
            }
        }
    }
    sip.finish128().as_u128()
}

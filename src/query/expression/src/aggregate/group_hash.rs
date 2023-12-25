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

use databend_common_hashtable::FastHash;
use ethnum::i256;

use crate::types::decimal::DecimalType;
use crate::types::ArgType;
use crate::types::BinaryType;
use crate::types::BitmapType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::Column;

const NULL_HASH_VAL: u64 = 0xd1cefa08eb382d69;

pub fn group_hash_columns(cols: &[Column]) -> Vec<u64> {
    debug_assert!(!cols.is_empty());

    let mut values = group_hash_column(&cols[0]);

    if cols.len() > 1 {
        for col in &cols[1..] {
            let col_values = group_hash_column(col);
            for (val, v) in values.iter_mut().zip(col_values) {
                *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ v;
            }
        }
    }
    values
}

pub fn group_hash_column(c: &Column) -> Vec<u64> {
    let len = c.len();
    match c.data_type() {
        DataType::Null => vec![NULL_HASH_VAL; len],
        DataType::EmptyArray => vec![NULL_HASH_VAL; len],
        DataType::EmptyMap => vec![NULL_HASH_VAL; len],
        DataType::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberDataType::NUM_TYPE => {
                group_hash_type_column::<NumberType<NUM_TYPE>>(c)
            }
        }),
        DataType::Decimal(v) => match v {
            DecimalDataType::Decimal128(_) => group_hash_type_column::<DecimalType<i128>>(c),
            DecimalDataType::Decimal256(_) => group_hash_type_column::<DecimalType<i256>>(c),
        },
        DataType::Boolean => group_hash_type_column::<BooleanType>(c),

        DataType::Binary => {
            let c = BinaryType::try_downcast_column(c).unwrap();
            BinaryType::iter_column(&c).map(|x| x.fast_hash()).collect()
        }
        DataType::String => {
            let c = StringType::try_downcast_column(c).unwrap();
            StringType::iter_column(&c).map(|x| x.fast_hash()).collect()
        }
        DataType::Bitmap => {
            let c = BitmapType::try_downcast_column(c).unwrap();
            BitmapType::iter_column(&c).map(|x| x.fast_hash()).collect()
        }
        DataType::Variant => {
            let c = VariantType::try_downcast_column(c).unwrap();
            VariantType::iter_column(&c)
                .map(|x| x.fast_hash())
                .collect()
        }

        DataType::Timestamp => group_hash_type_column::<TimestampType>(c),
        DataType::Date => group_hash_type_column::<DateType>(c),
        DataType::Nullable(_) => {
            let col = c.as_nullable().unwrap();
            let mut values = group_hash_column(&col.column);
            for (index, val) in col.validity.iter().enumerate() {
                if !val {
                    values[index] = NULL_HASH_VAL;
                }
            }
            values
        }
        DataType::Tuple(_) => todo!(),
        DataType::Array(_) => todo!(),
        DataType::Map(_) => todo!(),
        DataType::Generic(_) => unreachable!(),
    }
}

fn group_hash_type_column<T: ArgType>(col: &Column) -> Vec<u64>
where for<'a> T::ScalarRef<'a>: FastHash {
    let c = T::try_downcast_column(col).unwrap();
    T::iter_column(&c).map(|x| x.fast_hash()).collect()
}

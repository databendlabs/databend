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

use common_hashtable::FastHash;
use ethnum::i256;

use crate::types::decimal::DecimalType;
use crate::types::ArgType;
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

pub fn group_hash_columns(cols: &[Column], values: &mut [u64]) {
    debug_assert!(!cols.is_empty());

    combine_group_hash_column::<true>(&cols[0], values);
    if cols.len() > 1 {
        for col in &cols[1..] {
            combine_group_hash_column::<false>(&col, values);
        }
    }
}

pub fn combine_group_hash_column<const IS_FIRST: bool>(c: &Column, values: &mut [u64]) {
    match c.data_type() {
        DataType::Null => {}
        DataType::EmptyArray => {}
        DataType::EmptyMap => {}
        DataType::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberDataType::NUM_TYPE => {
                combine_group_hash_type_column::<IS_FIRST, NumberType<NUM_TYPE>>(c, values)
            }
        }),
        DataType::Decimal(v) => match v {
            DecimalDataType::Decimal128(_) => {
                combine_group_hash_type_column::<IS_FIRST, DecimalType<i128>>(c, values)
            }
            DecimalDataType::Decimal256(_) => {
                combine_group_hash_type_column::<IS_FIRST, DecimalType<i256>>(c, values)
            }
        },
        DataType::Boolean => combine_group_hash_type_column::<IS_FIRST, BooleanType>(c, values),

        DataType::String => {
            let c = StringType::try_downcast_column(c).unwrap();

            if IS_FIRST {
                for (x, val) in StringType::iter_column(&c).zip(values.iter_mut()) {
                    *val = x.fast_hash();
                }
            } else {
                for (x, val) in StringType::iter_column(&c).zip(values.iter_mut()) {
                    *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.fast_hash();
                }
            }
        }
        DataType::Bitmap => {
            let c = BitmapType::try_downcast_column(c).unwrap();
            if IS_FIRST {
                for (x, val) in BitmapType::iter_column(&c).zip(values.iter_mut()) {
                    *val = x.fast_hash();
                }
            } else {
                for (x, val) in BitmapType::iter_column(&c).zip(values.iter_mut()) {
                    *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.fast_hash();
                }
            }
        }
        DataType::Variant => {
            let c = VariantType::try_downcast_column(c).unwrap();
            if IS_FIRST {
                for (x, val) in VariantType::iter_column(&c).zip(values.iter_mut()) {
                    *val = x.fast_hash();
                }
            } else {
                for (x, val) in VariantType::iter_column(&c).zip(values.iter_mut()) {
                    *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.fast_hash();
                }
            }
        }

        DataType::Timestamp => combine_group_hash_type_column::<IS_FIRST, TimestampType>(c, values),
        DataType::Date => combine_group_hash_type_column::<IS_FIRST, DateType>(c, values),
        DataType::Nullable(_) => {
            let col = c.as_nullable().unwrap();
            if IS_FIRST {
                combine_group_hash_column::<IS_FIRST>(&col.column, values);
                for (val, ok) in values.iter_mut().zip(col.validity.iter()) {
                    if !ok {
                        *val = NULL_HASH_VAL;
                    }
                }
            } else {
                let mut values2 = vec![0; c.len()];
                combine_group_hash_column::<true>(&col.column, &mut values2);

                for ((x, val), ok) in values2
                    .iter()
                    .zip(values.iter_mut())
                    .zip(col.validity.iter())
                {
                    if ok {
                        *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ *x;
                    } else {
                        *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ NULL_HASH_VAL;
                    }
                }
            }
        }
        DataType::Tuple(_) => todo!(),
        DataType::Array(_) => todo!(),
        DataType::Map(_) => todo!(),
        DataType::Generic(_) => unreachable!(),
    }
}

fn combine_group_hash_type_column<const IS_FIRST: bool, T: ArgType>(
    col: &Column,
    values: &mut [u64],
) where
    for<'a> T::ScalarRef<'a>: FastHash,
{
    let c = T::try_downcast_column(col).unwrap();
    if IS_FIRST {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = x.fast_hash();
        }
    } else {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.fast_hash();
        }
    }
}

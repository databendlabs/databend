// Copyright 2020-2022 Jorge C. Leitão
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

use parquet2::indexes::PageIndex;

use super::ColumnPageStatistics;
use crate::arrow::array::Array;
use crate::arrow::array::FixedSizeBinaryArray;
use crate::arrow::array::MutableFixedSizeBinaryArray;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::datatypes::PrimitiveType;
use crate::arrow::trusted_len::TrustedLen;
use crate::arrow::types::i256;
use crate::arrow::types::NativeType;

pub fn deserialize(indexes: &[PageIndex<Vec<u8>>], data_type: DataType) -> ColumnPageStatistics {
    ColumnPageStatistics {
        min: deserialize_binary_iter(
            indexes.iter().map(|index| index.min.as_ref()),
            data_type.clone(),
        ),
        max: deserialize_binary_iter(indexes.iter().map(|index| index.max.as_ref()), data_type),
        null_count: PrimitiveArray::from_trusted_len_iter(
            indexes
                .iter()
                .map(|index| index.null_count.map(|x| x as u64)),
        ),
    }
}

fn deserialize_binary_iter<'a, I: TrustedLen<Item = Option<&'a Vec<u8>>>>(
    iter: I,
    data_type: DataType,
) -> Box<dyn Array> {
    match data_type.to_physical_type() {
        PhysicalType::Primitive(PrimitiveType::Int128) => {
            Box::new(PrimitiveArray::from_trusted_len_iter(iter.map(|v| {
                v.map(|x| {
                    // Copy the fixed-size byte value to the start of a 16 byte stack
                    // allocated buffer, then use an arithmetic right shift to fill in
                    // MSBs, which accounts for leading 1's in negative (two's complement)
                    // values.
                    let n = x.len();
                    let mut bytes = [0u8; 16];
                    bytes[..n].copy_from_slice(x);
                    i128::from_be_bytes(bytes) >> (8 * (16 - n))
                })
            })))
        }
        PhysicalType::Primitive(PrimitiveType::Int256) => {
            Box::new(PrimitiveArray::from_trusted_len_iter(iter.map(|v| {
                v.map(|x| {
                    let n = x.len();
                    let mut bytes = [0u8; 32];
                    bytes[..n].copy_from_slice(x);
                    i256::from_be_bytes(bytes)
                })
            })))
        }
        _ => {
            let mut a = MutableFixedSizeBinaryArray::try_new(
                data_type,
                Vec::with_capacity(iter.size_hint().0),
                None,
            )
            .unwrap();
            for item in iter {
                a.push(item);
            }
            let a: FixedSizeBinaryArray = a.into();
            Box::new(a)
        }
    }
}

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
use crate::arrow::array::BinaryArray;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::array::Utf8Array;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Error;
use crate::arrow::trusted_len::TrustedLen;

pub fn deserialize(
    indexes: &[PageIndex<Vec<u8>>],
    data_type: &DataType,
) -> Result<ColumnPageStatistics, Error> {
    Ok(ColumnPageStatistics {
        min: deserialize_binary_iter(indexes.iter().map(|index| index.min.as_ref()), data_type)?,
        max: deserialize_binary_iter(indexes.iter().map(|index| index.max.as_ref()), data_type)?,
        null_count: PrimitiveArray::from_trusted_len_iter(
            indexes
                .iter()
                .map(|index| index.null_count.map(|x| x as u64)),
        ),
    })
}

fn deserialize_binary_iter<'a, I: TrustedLen<Item = Option<&'a Vec<u8>>>>(
    iter: I,
    data_type: &DataType,
) -> Result<Box<dyn Array>, Error> {
    match data_type.to_physical_type() {
        PhysicalType::LargeBinary => Ok(Box::new(BinaryArray::<i64>::from_iter(iter))),
        PhysicalType::Utf8 => {
            let iter = iter.map(|x| x.map(|x| std::str::from_utf8(x)).transpose());
            Ok(Box::new(Utf8Array::<i32>::try_from_trusted_len_iter(iter)?))
        }
        PhysicalType::LargeUtf8 => {
            let iter = iter.map(|x| x.map(|x| std::str::from_utf8(x)).transpose());
            Ok(Box::new(Utf8Array::<i64>::try_from_trusted_len_iter(iter)?))
        }
        _ => Ok(Box::new(BinaryArray::<i32>::from_iter(iter))),
    }
}

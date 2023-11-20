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
use crate::arrow::array::BooleanArray;
use crate::arrow::array::PrimitiveArray;

pub fn deserialize(indexes: &[PageIndex<bool>]) -> ColumnPageStatistics {
    ColumnPageStatistics {
        min: Box::new(BooleanArray::from_trusted_len_iter(
            indexes.iter().map(|index| index.min),
        )),
        max: Box::new(BooleanArray::from_trusted_len_iter(
            indexes.iter().map(|index| index.max),
        )),
        null_count: PrimitiveArray::from_trusted_len_iter(
            indexes
                .iter()
                .map(|index| index.null_count.map(|x| x as u64)),
        ),
    }
}

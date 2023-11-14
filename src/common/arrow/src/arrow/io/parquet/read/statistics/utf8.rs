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

use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::Statistics as ParquetStatistics;

use crate::arrow::array::MutableArray;
use crate::arrow::array::MutableUtf8Array;
use crate::arrow::error::Result;
use crate::arrow::offset::Offset;

pub(super) fn push<O: Offset>(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutableUtf8Array<O>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutableUtf8Array<O>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<BinaryStatistics>().unwrap());

    min.push(
        from.and_then(|s| s.min_value.as_deref().map(simdutf8::basic::from_utf8))
            .transpose()?,
    );
    max.push(
        from.and_then(|s| s.max_value.as_deref().map(simdutf8::basic::from_utf8))
            .transpose()?,
    );
    Ok(())
}

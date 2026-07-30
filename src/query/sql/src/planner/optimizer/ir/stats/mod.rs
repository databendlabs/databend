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

mod column_stat;
mod constraint;
mod join;
mod selectivity;

pub use column_stat::*;
use databend_common_statistics::Datum;
pub use databend_common_statistics::UniformSampleSet;
pub(crate) use join::JoinConditionColumns;
pub(crate) use join::JoinKeyStatUpdate;
pub(crate) use join::JoinStatsEstimator;
pub use selectivity::MAX_SELECTIVITY;
pub use selectivity::SelectivityEstimator;

pub(crate) fn finite_range_ndv_upper(min: &Datum, max: &Datum) -> Option<f64> {
    if min == max {
        return Some(1.0);
    }
    match (min, max) {
        (Datum::Bool(false), Datum::Bool(true)) => Some(2.0),
        (Datum::Int(min), Datum::Int(max)) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|value| value as f64),
        (Datum::UInt(min), Datum::UInt(max)) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|value| value as f64),
        _ => None,
    }
}

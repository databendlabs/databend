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

use databend_common_arrow::arrow::bitmap::MutableBitmap;

use crate::arrow::bitmap_into_mut;
use crate::types::BooleanType;
use crate::Value;

pub struct FilterHelpers;

impl FilterHelpers {
    #[inline]
    pub fn is_all_unset(predicate: &Value<BooleanType>) -> bool {
        match &predicate {
            Value::Scalar(v) => !v,
            Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
        }
    }

    pub fn filter_to_bitmap(predicate: Value<BooleanType>, rows: usize) -> MutableBitmap {
        match predicate {
            Value::Scalar(true) => MutableBitmap::from_len_set(rows),
            Value::Scalar(false) => MutableBitmap::from_len_zeroed(rows),
            Value::Column(bitmap) => bitmap_into_mut(bitmap),
        }
    }
}

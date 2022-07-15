// Copyright 2022 Datafuse Labs.
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

use common_metrics::label_counter_with_val_and_labels;

pub static METRIC_DATA_BLOCK_CREATED: &str = "data_block_created_total";
pub static METRIC_DATA_BLOCK_DROPPED: &str = "data_block_dropped_total";

#[inline]
pub fn incr_data_block_created() {
    label_counter_with_val_and_labels(METRIC_DATA_BLOCK_CREATED, vec![], 1);
}

#[inline]
pub fn incr_data_block_dropped() {
    label_counter_with_val_and_labels(METRIC_DATA_BLOCK_DROPPED, vec![], 1);
}

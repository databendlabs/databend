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

use metrics::counter;

pub static METRIC_DATA_BLOCK_CREATED: &str = "data_block_created_total";
pub static METRIC_DATA_BLOCK_DROPPED: &str = "data_block_dropped_total";

fn incr_created_counter() {
    counter!(super::metrics::METRIC_DATA_BLOCK_CREATED, 1);
}

fn incr_dropped_counter() {
    counter!(super::metrics::METRIC_DATA_BLOCK_DROPPED, 1);
}

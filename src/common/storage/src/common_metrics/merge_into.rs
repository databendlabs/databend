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

use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("query_", $key)
    };
}

pub fn metrics_inc_merge_into_replace_blocks_counter(c: u32) {
    increment_gauge!(key!("merge_into_replace_blocks_counter"), c as f64);
}

pub fn metrics_inc_merge_into_append_blocks_counter(c: u32) {
    increment_gauge!(key!("merge_into_append_blocks_counter"), c as f64);
}

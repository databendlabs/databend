// Copyright 2023 Datafuse Labs
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

// interpreter metrics

use metrics::increment_gauge;

// the time used in executing the whole replace-into statement
pub fn metrics_inc_replace_execution_time_ms(c: u64) {
    increment_gauge!("replace_into_time_execution_ms", c as f64);
}

// the time used in executing the mutation (upsert) part of the replace-into statement
pub fn metrics_inc_replace_mutation_time_ms(c: u64) {
    increment_gauge!("replace_into_time_mutation_ms", c as f64);
}

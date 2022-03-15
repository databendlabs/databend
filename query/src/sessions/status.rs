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

use std::time::SystemTime;

#[derive(Default, Clone)]
pub struct Status {
    pub running_queries_count: u64,
    pub last_query_started_at: Option<SystemTime>,
    pub last_query_finished_at: Option<SystemTime>,
}

impl Status {
    pub(crate) fn query_start(&mut self, now: SystemTime) {
        self.running_queries_count += 1;
        self.last_query_started_at = Some(now)
    }

    pub(crate) fn query_finish(&mut self, now: SystemTime) {
        self.running_queries_count -= 1;
        if self.running_queries_count == 0 {
            self.last_query_finished_at = Some(now)
        }
    }
}

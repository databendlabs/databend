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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_exception::Result;
use databend_common_storages_system::ClusteringHistoryLogElement;
use databend_common_storages_system::ClusteringHistoryQueue;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InterpreterClusteringHistory;

impl InterpreterClusteringHistory {
    pub fn write_log(
        ctx: &QueryContext,
        start: SystemTime,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        ClusteringHistoryQueue::instance()?.append_data(ClusteringHistoryLogElement {
            start_time: start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_micros() as i64,
            end_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_micros() as i64,
            database: db_name.to_string(),
            table: table_name.to_string(),
            byte_size: ctx.get_scan_progress_value().bytes as u64,
            row_count: ctx.get_scan_progress_value().rows as u64,
        })
    }
}

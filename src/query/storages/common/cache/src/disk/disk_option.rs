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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Settings of the disk cache writer, from `cache.disk` and changed at runtime by
/// `system$set_cache_capacity('disk.<name>', <value>)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskOption {
    /// Bytes waiting to be written, at most.
    QueueBytes,
    /// Milliseconds a read waits for room in a full queue before giving up the fill.
    QueueWaitMs,
    /// Percent of the file system in use above which entries evict before they are written.
    WatermarkRatio,
    /// Whether written entries are synced to disk, 0 or 1.
    SyncData,
}

impl DiskOption {
    pub const ALL: [DiskOption; 4] = [
        DiskOption::QueueBytes,
        DiskOption::QueueWaitMs,
        DiskOption::WatermarkRatio,
        DiskOption::SyncData,
    ];

    /// As in `cache.disk` and after `disk.` in SQL.
    pub fn name(self) -> &'static str {
        match self {
            DiskOption::QueueBytes => "queue_bytes",
            DiskOption::QueueWaitMs => "queue_wait_ms",
            DiskOption::WatermarkRatio => "watermark_ratio",
            DiskOption::SyncData => "sync_data",
        }
    }

    pub fn from_name(name: &str) -> Option<DiskOption> {
        DiskOption::ALL.into_iter().find(|o| o.name() == name)
    }

    /// Fails for a value out of the option's range.
    pub fn check(self, value: u64) -> Result<()> {
        let valid = match self {
            DiskOption::QueueBytes => value > 0,
            DiskOption::QueueWaitMs => true,
            DiskOption::WatermarkRatio => (1..=100).contains(&value),
            DiskOption::SyncData => value <= 1,
        };
        if valid {
            return Ok(());
        }
        let range = match self {
            DiskOption::QueueBytes => "more than 0",
            DiskOption::QueueWaitMs => "any number",
            DiskOption::WatermarkRatio => "1 to 100",
            DiskOption::SyncData => "0 or 1",
        };
        Err(ErrorCode::BadArguments(format!(
            "cache.disk.{} must be {range}, got {value}",
            self.name()
        )))
    }
}

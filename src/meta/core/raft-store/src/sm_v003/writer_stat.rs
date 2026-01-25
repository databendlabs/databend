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

use std::fmt;

use log::info;

/// The progress stat about snapshot writer.
pub(crate) struct WriterStat {
    /// Number of entries written.
    pub(crate) cnt: u64,

    /// The count of entries to reach before next progress logging.
    next_progress_cnt: u64,

    /// The time when the writer starts to write entries.
    start_time: std::time::Instant,
}

impl fmt::Display for WriterStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{cnt:{}, elapsed:{:?}}}",
            self.cnt,
            self.start_time.elapsed()
        )
    }
}

impl WriterStat {
    pub(crate) fn new() -> Self {
        WriterStat {
            cnt: 0,
            next_progress_cnt: 1000,
            start_time: std::time::Instant::now(),
        }
    }

    /// Increase the number of entries written by one.
    pub(crate) fn inc(&mut self) {
        self.cnt += 1;

        if self.cnt == self.next_progress_cnt {
            self.log_progress();

            // Increase the number of entries before next log by 5%,
            // but at least 50k, at most 800k.
            let step = std::cmp::min(self.next_progress_cnt / 20, 800_000);
            let step = std::cmp::max(step, 50_000);

            self.next_progress_cnt += step;
        }
    }

    pub(crate) fn log_progress(&self) {
        let elapsed_sec = self.start_time.elapsed().as_secs();
        // Avoid div by 0
        let avg = self.cnt / (elapsed_sec + 1);

        if self.cnt >= 10_000_000 {
            info!(
                "Snapshot Writer has written {} million entries; avg: {} kilo entries/s",
                self.cnt / 1_000_000,
                avg / 1_000,
            )
        } else {
            info!(
                "Snapshot Writer has written {} kilo entries; avg: {} kilo entries/s",
                self.cnt / 1_000,
                avg / 1_000,
            )
        }
    }
}

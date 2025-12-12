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

use databend_common_base::runtime::ThreadTracker;
use logforth::filter::FilterResult;

/// ThreadTrackerFilter is used in two scenarios:
/// - `report issues`: capture logs according to `CaptureLogSettings::capture_query`.
/// - `system history tables`: filter background SQL produced logs via `CaptureLogSettings::capture_off`.
#[derive(Debug)]
pub struct ThreadTrackerFilter {
    pub report_issues: bool,
}

impl logforth::Filter for ThreadTrackerFilter {
    fn enabled(&self, metadata: &log::Metadata) -> FilterResult {
        if let Some(settings) = ThreadTracker::capture_log_settings() {
            if metadata.level() > settings.level {
                return FilterResult::Reject;
            }
            return FilterResult::Neutral;
        }

        // For `report_issues` this is the only filter (so neutral means accept).
        // Outside the `report_issues` scope, we must reject to keep `log_enabled!` accurate—
        // it checks whether any dispatcher accepts the record.
        if self.report_issues {
            return FilterResult::Reject;
        }

        FilterResult::Neutral
    }
}

impl Default for ThreadTrackerFilter {
    fn default() -> Self {
        Self {
            report_issues: false,
        }
    }
}

impl ThreadTrackerFilter {
    /// Enable the `report issues` mode on this filter.
    pub fn report_issues(mut self) -> Self {
        self.report_issues = true;
        self
    }
}

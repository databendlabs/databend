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
use logforth::filter::CustomFilter;
use logforth::filter::FilterResult;

pub fn filter_by_thread_tracker() -> CustomFilter {
    CustomFilter::new(|metadata| {
        if let Some(settings) = ThreadTracker::capture_log_settings() {
            if metadata.level() > settings.level {
                return FilterResult::Reject;
            }
        }

        FilterResult::Neutral
    })
}

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

use std::time::Duration;

use backoff::ExponentialBackoff;
use backoff::ExponentialBackoffBuilder;

const OCC_DEFAULT_BACKOFF_INIT_DELAY_MS: Duration = Duration::from_millis(5);
const OCC_DEFAULT_BACKOFF_MAX_DELAY_MS: Duration = Duration::from_millis(20 * 1000);
const OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS: Duration = Duration::from_millis(120 * 1000);

#[inline]
pub fn set_backoff(
    init_retry_delay: Option<Duration>,
    max_retry_delay: Option<Duration>,
    max_retry_elapsed: Option<Duration>,
) -> ExponentialBackoff {
    // The initial retry delay in millisecond. By default,  it is 5 ms.
    let init_delay = init_retry_delay.unwrap_or(OCC_DEFAULT_BACKOFF_INIT_DELAY_MS);

    // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
    // By default, it is 20 seconds.
    let max_delay = max_retry_delay.unwrap_or(OCC_DEFAULT_BACKOFF_MAX_DELAY_MS);

    // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
    // By default, it is 2 minutes
    let max_elapsed = max_retry_elapsed.unwrap_or(OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS);

    // TODO(xuanwo): move to backon instead.
    //
    // To simplify the settings, using fixed common values for randomization_factor and multiplier
    ExponentialBackoffBuilder::new()
        .with_initial_interval(init_delay)
        .with_max_interval(max_delay)
        .with_randomization_factor(0.5)
        .with_multiplier(2.0)
        .with_max_elapsed_time(Some(max_elapsed))
        .build()
}

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

use databend_common_exception::Result;
use databend_common_settings::Settings;

// The two endpoints can observe the same broken stream at slightly different times. The receiver
// lease must outlive the sender deadline so a replacement connection cannot race with expiry.
const RECEIVER_LEASE_MARGIN_SECS: u64 = 5;

#[derive(Clone, Copy)]
pub(crate) struct FlightReconnectPolicy {
    pub(crate) retry_times: u64,
    pub(crate) retry_interval: Duration,
    pub(crate) retry_timeout: Duration,
    pub(crate) receiver_lease: Duration,
}

impl FlightReconnectPolicy {
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self> {
        let retry_times = settings.get_flight_max_retry_times()?;
        let retry_interval_secs = settings.get_flight_retry_interval()?;
        // Backoff must not consume the request-time budget before all configured attempts run.
        let retry_timeout_secs = settings
            .get_flight_client_timeout()?
            .saturating_add(retry_interval_secs.saturating_mul(retry_times));
        let receiver_lease_secs = if retry_times == 0 {
            0
        } else {
            retry_timeout_secs.saturating_add(std::cmp::max(
                retry_interval_secs,
                RECEIVER_LEASE_MARGIN_SECS,
            ))
        };

        Ok(Self {
            retry_times,
            retry_interval: Duration::from_secs(retry_interval_secs),
            retry_timeout: Duration::from_secs(retry_timeout_secs),
            receiver_lease: Duration::from_secs(receiver_lease_secs),
        })
    }
}

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

const RECEIVER_LEASE_MARGIN: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
pub(crate) struct FlightReconnectPolicy {
    pub(crate) retry_times: u64,
    pub(crate) retry_interval: Duration,
    pub(crate) timeout: Duration,
}

impl FlightReconnectPolicy {
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self> {
        let retry_times = settings.get_flight_max_retry_times()?;
        let retry_interval_secs = settings.get_flight_retry_interval()?;
        let timeout_secs = settings.get_flight_client_timeout()?;

        Ok(Self {
            retry_times,
            retry_interval: Duration::from_secs(retry_interval_secs),
            timeout: Duration::from_secs(timeout_secs),
        })
    }

    pub(crate) fn receiver_lease(self) -> Duration {
        if self.retry_times == 0 {
            return Duration::ZERO;
        }
        let retry_times = self.retry_times.min(u32::MAX as u64) as u32;
        self.timeout
            .saturating_add(self.retry_interval.saturating_mul(retry_times))
            .saturating_add(std::cmp::max(self.retry_interval, RECEIVER_LEASE_MARGIN))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_receiver_lease_includes_retry_grace() {
        let policy = FlightReconnectPolicy {
            retry_times: 10,
            retry_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(60),
        };

        assert_eq!(policy.receiver_lease(), Duration::from_secs(75));
    }

    #[test]
    fn test_receiver_lease_is_disabled_without_retries() {
        let policy = FlightReconnectPolicy {
            retry_times: 0,
            retry_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(60),
        };

        assert_eq!(policy.receiver_lease(), Duration::ZERO);
    }
}

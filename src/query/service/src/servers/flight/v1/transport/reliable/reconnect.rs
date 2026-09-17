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
pub struct FlightReconnectPolicy {
    retry_times: u64,
    pub(crate) retry_interval: Duration,
    pub(crate) timeout: Duration,
}

#[derive(Clone, Copy)]
pub(crate) struct FlightConnectionAttempts {
    remaining: u64,
}

impl FlightConnectionAttempts {
    pub(crate) fn remaining(self) -> u64 {
        self.remaining
    }

    pub(crate) fn is_empty(self) -> bool {
        self.remaining == 0
    }

    pub(crate) fn consume(mut self, used: u64) -> Self {
        self.remaining = self
            .remaining
            .checked_sub(used)
            .expect("connection attempts used must not exceed the available budget");
        self
    }

    fn max_elapsed(self, timeout: Duration, retry_interval: Duration) -> Duration {
        let attempts = self.remaining.min(u32::MAX as u64) as u32;
        let intervals = attempts.saturating_sub(1);
        timeout
            .saturating_mul(attempts)
            .saturating_add(retry_interval.saturating_mul(intervals))
    }
}

impl FlightReconnectPolicy {
    pub fn new(retry_times: u64, retry_interval: Duration, timeout: Duration) -> Self {
        Self {
            retry_times,
            retry_interval,
            timeout,
        }
    }

    /// Returns the reconnect policy for New Flight, or `None` when the query keeps the
    /// existing Flight path. Production query setup is the only caller.
    pub fn from_settings(settings: &Settings) -> Result<Option<Self>> {
        if !settings.get_enable_experiment_new_flight()? {
            return Ok(None);
        }

        Ok(Some(Self::new(
            settings.get_flight_max_retry_times()?,
            Duration::from_secs(settings.get_flight_retry_interval()?),
            Duration::from_secs(settings.get_flight_client_timeout()?),
        )))
    }

    pub fn receiver_lease_secs(self) -> u64 {
        self.receiver_lease().as_secs()
    }

    pub(crate) fn initial_attempts(self) -> FlightConnectionAttempts {
        FlightConnectionAttempts {
            remaining: self.retry_times.saturating_add(1),
        }
    }

    pub(crate) fn reconnect_attempts(self) -> FlightConnectionAttempts {
        FlightConnectionAttempts {
            remaining: self.retry_times,
        }
    }

    pub fn receiver_lease(self) -> Duration {
        let attempts = self.reconnect_attempts();
        if attempts.is_empty() {
            return Duration::ZERO;
        }

        attempts
            .max_elapsed(self.timeout, self.retry_interval)
            .saturating_add(std::cmp::max(self.retry_interval, RECEIVER_LEASE_MARGIN))
    }
}

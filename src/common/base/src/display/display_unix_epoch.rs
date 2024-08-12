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
use std::time::Duration;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use chrono::Utc;

pub struct DisplayUnixTimeStamp {
    /// The duration since the UNIX epoch.
    duration: Duration,
}

impl fmt::Display for DisplayUnixTimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let system_time = UNIX_EPOCH + self.duration;
        let datetime: DateTime<Utc> = system_time.into();

        write!(f, "{}", datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ%z"))
    }
}

pub trait DisplayUnixTimeStampExt {
    fn display_unix_timestamp(&self) -> DisplayUnixTimeStamp;
}

impl DisplayUnixTimeStampExt for Duration {
    fn display_unix_timestamp(&self) -> DisplayUnixTimeStamp {
        DisplayUnixTimeStamp { duration: *self }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_display_unix_epoch() {
        let epoch = Duration::from_millis(0);
        let display = epoch.display_unix_timestamp();
        assert_eq!(format!("{}", display), "1970-01-01T00:00:00.000000Z+0000");

        let epoch = Duration::from_millis(1723102819023);
        let display = epoch.display_unix_timestamp();
        assert_eq!(format!("{}", display), "2024-08-08T07:40:19.023000Z+0000");
    }
}

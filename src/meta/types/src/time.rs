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

use std::ops::Add;
use std::ops::Sub;
use std::time::Duration;

/// A interval of time.
///
/// As a replacement of [`Duration`], which is not `serde`-able.
///
/// `Interval` implements: `Interval +- Interval`.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
)]
pub struct Interval {
    pub(crate) millis: u64,
}

impl Interval {
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            millis: duration.as_millis() as u64,
        }
    }

    pub fn from_millis(millis: u64) -> Self {
        Self::from_duration(Duration::from_millis(millis))
    }

    pub fn from_secs(secs: u64) -> Self {
        Self::from_duration(Duration::from_secs(secs))
    }

    pub fn millis(&self) -> u64 {
        self.millis
    }

    pub fn seconds(&self) -> u64 {
        self.millis / 1000
    }
}

impl Add for Interval {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            millis: self.millis.saturating_add(rhs.millis),
        }
    }
}

impl Sub for Interval {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            millis: self.millis.saturating_sub(rhs.millis),
        }
    }
}

/// A time point since 1970-01-01.
///
/// As a replacement of [`Instant`](std::time::Instant), which is not `serde`-able.
/// `Time` implements: `Time +- Interval = Time` and `Time - Time = Interval`.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
)]
pub struct Time {
    pub(crate) time: Interval,
}

impl Time {
    pub fn from_millis(millis: u64) -> Self {
        Self {
            time: Interval::from_millis(millis),
        }
    }

    pub fn from_secs(secs: u64) -> Self {
        Self {
            time: Interval::from_secs(secs),
        }
    }

    pub fn millis(&self) -> u64 {
        self.time.millis()
    }

    pub fn seconds(&self) -> u64 {
        self.time.seconds()
    }
}

impl Add<Interval> for Time {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        Self {
            time: self.time + rhs,
        }
    }
}

impl Sub<Interval> for Time {
    type Output = Self;

    fn sub(self, rhs: Interval) -> Self::Output {
        Self {
            time: self.time - rhs,
        }
    }
}

impl Sub for Time {
    type Output = Interval;

    fn sub(self, rhs: Self) -> Self::Output {
        self.time - rhs.time
    }
}

#[cfg(test)]
mod tests {
    use super::Interval;
    use crate::time::Time;

    #[test]
    fn test_interval() {
        let interval = Interval::from_millis(1000);
        assert_eq!(interval.millis(), 1000);
        assert_eq!(interval.seconds(), 1);

        let interval = Interval::from_secs(1);
        assert_eq!(interval.millis(), 1000);
        assert_eq!(interval.seconds(), 1);

        assert_eq!(interval + interval, Interval::from_millis(2000));
        assert_eq!(interval - interval, Interval::from_millis(0));
        assert_eq!(
            interval - Interval::from_millis(1500),
            Interval::from_millis(0)
        );
    }

    #[test]
    fn test_time() {
        let time = Time::from_millis(1000);
        assert_eq!(time.millis(), 1000);
        assert_eq!(time.seconds(), 1);

        let time = Time::from_secs(1);
        assert_eq!(time.millis(), 1000);
        assert_eq!(time.seconds(), 1);

        assert_eq!(time + Interval::from_millis(1000), Time::from_millis(2000));
        assert_eq!(time - Interval::from_millis(500), Time::from_millis(500));
        assert_eq!(time - Time::from_millis(500), Interval::from_millis(500));
        assert_eq!(time - Time::from_millis(1500), Interval::from_millis(0));
    }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Temporal value conversions for dates, times, and timestamps

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc};

pub(crate) mod date {
    use super::*;

    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )
        .num_days() as i32
    }

    pub(crate) fn days_to_date(days: i32) -> NaiveDate {
        // This shouldn't fail until the year 262000
        (chrono::DateTime::UNIX_EPOCH + TimeDelta::try_days(days as i64).unwrap())
            .naive_utc()
            .date()
    }

    /// Returns unix epoch.
    pub(crate) fn unix_epoch() -> DateTime<Utc> {
        Utc.timestamp_nanos(0)
    }

    /// Creates date literal from `NaiveDate`, assuming it's utc timezone.
    pub(crate) fn date_from_naive_date(date: NaiveDate) -> i32 {
        (date - unix_epoch().date_naive()).num_days() as i32
    }
}

pub(crate) mod time {
    use super::*;

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
        )
        .num_microseconds()
        .unwrap()
    }

    pub(crate) fn microseconds_to_time(micros: i64) -> NaiveTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1_000).unwrap()
    }
}

pub(crate) mod timestamp {
    use super::*;

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> i64 {
        time.and_utc().timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetime(micros: i64) -> NaiveDateTime {
        // This shouldn't fail until the year 262000
        DateTime::from_timestamp_micros(micros).unwrap().naive_utc()
    }

    pub(crate) fn nanoseconds_to_datetime(nanos: i64) -> NaiveDateTime {
        DateTime::from_timestamp_nanos(nanos).naive_utc()
    }
}

pub(crate) mod timestamptz {
    use super::*;

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetimetz(micros: i64) -> DateTime<Utc> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        DateTime::from_timestamp(secs, rem as u32 * 1_000).unwrap()
    }

    pub(crate) fn nanoseconds_to_datetimetz(nanos: i64) -> DateTime<Utc> {
        let (secs, rem) = (nanos / 1_000_000_000, nanos % 1_000_000_000);

        DateTime::from_timestamp(secs, rem as u32).unwrap()
    }
}

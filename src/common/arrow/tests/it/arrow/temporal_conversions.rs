// Copyright 2020-2022 Jorge C. Leitão
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

use chrono::NaiveDateTime;
use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::datatypes::TimeUnit;
use databend_common_arrow::arrow::temporal_conversions;
use databend_common_arrow::arrow::types::months_days_ns;

#[test]
fn naive() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S:z";
    let array = Utf8Array::<i32>::from_slice([
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{r:?}"), expected);

    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice([
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{r:?}"), expected);
}

#[test]
fn naive_scalar() {
    let fmt = "%Y-%m-%dT%H:%M:%S.%9f%z";
    let str = "2023-04-07T12:23:34.123456789Z";

    let nanos_expected = 1680870214123456789;

    // seconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Second);
    assert_eq!(r, Some(nanos_expected / 1_000_000_000));
    // milliseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Millisecond);
    assert_eq!(r, Some(nanos_expected / 1_000_000));
    // microseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Microsecond);
    assert_eq!(r, Some(nanos_expected / 1_000));
    // nanoseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Nanosecond);
    assert_eq!(r, Some(nanos_expected));
}

#[test]
fn naive_scalar_no_tz() {
    let fmt = "%Y-%m-%dT%H:%M:%S.%9f";

    let str = "2023-04-07T12:23:34.123456789";
    let nanos_expected = 1680870214123456789;

    // seconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Second);
    assert_eq!(r, Some(nanos_expected / 1_000_000_000));
    // milliseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Millisecond);
    assert_eq!(r, Some(nanos_expected / 1_000_000));
    // microseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Microsecond);
    assert_eq!(r, Some(nanos_expected / 1_000));
    // nanoseconds
    let r = temporal_conversions::utf8_to_naive_timestamp_scalar(str, fmt, &TimeUnit::Nanosecond);
    assert_eq!(r, Some(nanos_expected));
}

#[test]
fn scalar_tz_aware() {
    let fmt = "%Y-%m-%dT%H:%M:%S%.f%:z";

    let tz = temporal_conversions::parse_offset("-02:00").unwrap();
    let str = "2023-04-07T10:23:34.000000000-02:00";
    let nanos_expected = 1680870214000000000;

    // seconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Second);
    assert_eq!(r, Some(nanos_expected / 1_000_000_000));
    // milliseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Millisecond);
    assert_eq!(r, Some(nanos_expected / 1_000_000));
    // microseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Microsecond);
    assert_eq!(r, Some(nanos_expected / 1_000));
    // nanoseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Nanosecond);
    assert_eq!(r, Some(nanos_expected));
}
#[test]
fn scalar_tz_aware_no_timezone() {
    let fmt = "%Y-%m-%dT%H:%M:%S%.f";

    let tz = temporal_conversions::parse_offset("-02:00").unwrap();
    let str = "2023-04-07T10:23:34.000000000-02:00";

    // seconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Second);
    assert_eq!(r, None);
    // milliseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Millisecond);
    assert_eq!(r, None);
    // microseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Microsecond);
    assert_eq!(r, None);
    // nanoseconds
    let r = temporal_conversions::utf8_to_timestamp_scalar(str, fmt, &tz, &TimeUnit::Nanosecond);
    assert_eq!(r, None);
}

#[test]
fn naive_no_tz() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice([
        "1996-12-19T16:39:57",
        "1996-12-19T13:39:57",
        "1996-12-19 13:39:57", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{r:?}"), expected);
}

#[test]
fn timestamp_to_datetime() {
    let fmt = "%Y-%m-%dT%H:%M:%S.%9f";
    let ts = 1680870214123456789;

    // positive milliseconds
    assert_eq!(
        temporal_conversions::timestamp_ms_to_datetime(ts / 1_000_000),
        NaiveDateTime::parse_from_str("2023-04-07T12:23:34.123000000", fmt).unwrap()
    );
    // positive microseconds
    assert_eq!(
        temporal_conversions::timestamp_us_to_datetime(ts / 1_000),
        NaiveDateTime::parse_from_str("2023-04-07T12:23:34.123456000", fmt).unwrap()
    );
    // positive nanoseconds
    assert_eq!(
        temporal_conversions::timestamp_ns_to_datetime(ts),
        NaiveDateTime::parse_from_str("2023-04-07T12:23:34.123456789", fmt).unwrap()
    );

    let ts = -15548276987654321;

    // negative milliseconds
    assert_eq!(
        temporal_conversions::timestamp_ms_to_datetime(ts / 1_000_000),
        NaiveDateTime::parse_from_str("1969-07-05T01:02:03.013000000", fmt).unwrap()
    );
    // negative microseconds
    assert_eq!(
        temporal_conversions::timestamp_us_to_datetime(ts / 1_000),
        NaiveDateTime::parse_from_str("1969-07-05T01:02:03.012346000", fmt).unwrap()
    );
    // negative nanoseconds
    assert_eq!(
        temporal_conversions::timestamp_ns_to_datetime(ts),
        NaiveDateTime::parse_from_str("1969-07-05T01:02:03.012345679", fmt).unwrap()
    );

    let fmt = "%Y-%m-%dT%H:%M:%S";
    let ts = -2209075200000000000;
    let expected = NaiveDateTime::parse_from_str("1899-12-31T00:00:00", fmt).unwrap();

    assert_eq!(
        temporal_conversions::timestamp_ms_to_datetime(ts / 1_000_000),
        expected
    );
    assert_eq!(
        temporal_conversions::timestamp_us_to_datetime(ts / 1_000),
        expected
    );
    assert_eq!(temporal_conversions::timestamp_ns_to_datetime(ts), expected);
}

#[test]
fn timestamp_to_negative_datetime() {
    let fmt = "%Y-%m-%d %H:%M:%S";
    let ts = -63135596800000000;
    let expected = NaiveDateTime::parse_from_str("-0031-04-24 22:13:20", fmt).unwrap();

    assert_eq!(
        temporal_conversions::timestamp_ms_to_datetime(ts / 1_000),
        expected
    );
    assert_eq!(temporal_conversions::timestamp_us_to_datetime(ts), expected);
}

#[test]
fn tz_aware() {
    let tz = "-02:00".to_string();
    let expected = "Timestamp(Nanosecond, Some(\"-02:00\"))[1996-12-19 16:39:57 -02:00, 1996-12-19 17:39:57 -02:00, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f%:z";
    let array = Utf8Array::<i32>::from_slice([
        "1996-12-19T16:39:57.0-02:00",
        "1996-12-19T16:39:57.0-03:00", // same time at a different TZ
        "1996-12-19 13:39:57.0-03:00",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{r:?}"), expected);
}

#[test]
fn tz_aware_no_timezone() {
    let tz = "-02:00".to_string();
    let expected = "Timestamp(Nanosecond, Some(\"-02:00\"))[None, None, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f";
    let array = Utf8Array::<i32>::from_slice([
        "1996-12-19T16:39:57.0",
        "1996-12-19T17:39:57.0",
        "1996-12-19 13:39:57.0",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{r:?}"), expected);
}

#[test]
fn add_interval_fixed_offset() {
    // 1972 has a leap year on the 29th.
    let timestamp = 68086800; // Mon Feb 28 1972 01:00:00 GMT+0000
    let timeunit = TimeUnit::Second;
    let timezone = temporal_conversions::parse_offset("+01:00").unwrap();

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(0, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-02-29 02:01:00 +01:00", format!("{r}"));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(1, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-03-29 02:01:00 +01:00", format!("{r}"));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(24, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1974-03-01 02:01:00 +01:00", format!("{r}"));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(-1, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-01-29 02:01:00 +01:00", format!("{r}"));
}

#[cfg(feature = "chrono-tz")]
#[test]
fn add_interval_timezone() {
    // current time is Sun Mar 29 2020 00:00:00 GMT+0000 (Western European Standard Time)
    // 1 hour later is Sun Mar 29 2020 02:00:00 GMT+0100 (Western European Summer Time)
    let timestamp = 1585440000;
    let timeunit = TimeUnit::Second;
    let timezone = temporal_conversions::parse_offset_tz("Europe/Lisbon").unwrap();

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(0, 0, 60 * 60 * 1_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("2020-03-29 02:00:00 WEST", format!("{r}"));

    // crosses two summer time changes and thus adds only 1 hour
    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(7, 0, 60 * 60 * 1_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("2020-10-29 01:00:00 WET", format!("{r}"));
}

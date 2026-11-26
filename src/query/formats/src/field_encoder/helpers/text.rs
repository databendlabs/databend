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

use chrono_tz::Tz;
use databend_common_expression::types::date::DATE_FORMAT;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::timestamp::TIMESTAMP_FORMAT;
use databend_common_expression::types::timestamp::timestamp_from_micros;

/// Adapts a byte or string encoder's text sink to Rust formatting.
pub(crate) struct TextWriter<F>(pub F);

impl<F: FnMut(&str)> fmt::Write for TextWriter<F> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        (self.0)(value);
        Ok(())
    }
}

pub(crate) fn format_date(value: i32) -> impl fmt::Display {
    let date = date_from_days(value);
    fmt::from_fn(move |out| date.format(DATE_FORMAT).write_to(out))
}

pub(crate) fn format_timestamp(value: i64, timezone: &Tz) -> impl fmt::Display + use<> {
    // The format contains no timezone specifier. Formatting the local datetime
    // avoids building chrono's timezone name as well as its Display buffer.
    let datetime = timestamp_from_micros(value, timezone).naive_local();
    fmt::from_fn(move |out| datetime.format(TIMESTAMP_FORMAT).write_to(out))
}

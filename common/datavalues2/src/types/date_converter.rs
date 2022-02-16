// Copyright 2021 Datafuse Labs.
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

use chrono::Date;
use chrono::DateTime;
use chrono::Duration;
use chrono::TimeZone;
use chrono_tz::Tz;
use num::cast::AsPrimitive;

pub trait DateConverter {
    fn to_date(&self, tz: &Tz) -> Date<Tz>;
    fn to_date_time(&self, tz: &Tz) -> DateTime<Tz>;
    fn to_date_time64(&self, precision: usize, tz: &Tz) -> DateTime<Tz>;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, tz: &Tz) -> Date<Tz> {
        let mut dt = tz.ymd(1970, 1, 1);
        dt = dt.checked_add_signed(Duration::days(self.as_())).unwrap();
        dt
    }

    fn to_date_time(&self, tz: &Tz) -> DateTime<Tz> {
        tz.timestamp_millis(self.as_() * 1000)
    }

    fn to_date_time64(&self, precision: usize, tz: &Tz) -> DateTime<Tz> {
        let base10: i64 = 10;
        let nano = self.as_() * base10.pow(9 - precision as u32);

        let sec = nano / 1_000_000_000;
        let nsec = nano % 1_000_000_000;

        tz.timestamp(sec, nsec as u32)
    }
}

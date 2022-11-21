// Copyright 2022 Datafuse Labs.
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

use std::ops::AddAssign;

use chrono::Duration;
use chrono::NaiveDate;

const DATE_FMT: &str = "%Y-%m-%d";

pub fn date_to_string(v: &i64) -> String {
    let mut date = NaiveDate::from_ymd(1970, 1, 1);
    let d = Duration::days(*v);
    date.add_assign(d);
    date.format(DATE_FMT).to_string()
}

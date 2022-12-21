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

use chrono_tz::Tz;
use common_expression::date_helper::DateConverter;

const TIME_FMT_MICRO: &str = "%Y-%m-%d %H:%M:%S%.6f";

pub fn timestamp_to_string_micro(v: &i64, tz: Tz) -> String {
    let dt = v.to_timestamp(tz);
    dt.format(TIME_FMT_MICRO).to_string()
}

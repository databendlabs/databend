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

use crate::display::display_date;

/// date ranges from 1000-01-01 to 9999-12-31
/// date_max and date_min means days offset from 1970-01-01
/// any date not in the range will be invalid
pub const DATE_MAX: i32 = 2932896;
pub const DATE_MIN: i32 = -354285;

#[inline]
pub fn check_date(days: i64) -> Result<(), String> {
    if (DATE_MIN as i64..=DATE_MAX as i64).contains(&days) {
        return Ok(());
    }
    Err(format!("date `{}` is out of range", display_date(days)))
}

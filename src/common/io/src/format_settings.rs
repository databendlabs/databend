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

use chrono_tz::Tz;
use jiff::tz::TimeZone;

use crate::GeometryDataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatSettings {
    pub timezone: Tz,
    pub jiff_timezone: TimeZone,
    pub geometry_format: GeometryDataType,
    pub enable_dst_hour_fix: bool,
    pub format_null_as_str: bool,
}

// only used for tests
impl Default for FormatSettings {
    fn default() -> Self {
        Self {
            timezone: "UTC".parse::<Tz>().unwrap(),
            jiff_timezone: TimeZone::UTC,
            geometry_format: GeometryDataType::default(),
            enable_dst_hour_fix: false,
            format_null_as_str: false,
        }
    }
}

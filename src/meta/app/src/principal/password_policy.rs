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

use chrono::DateTime;
use chrono::Utc;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PasswordPolicy {
    pub name: String,
    pub min_length: u64,
    pub max_length: u64,
    pub min_upper_case_chars: u64,
    pub min_lower_case_chars: u64,
    pub min_numeric_chars: u64,
    pub min_special_chars: u64,
    pub min_age_days: u64,
    pub max_age_days: u64,
    pub max_retries: u64,
    pub lockout_time_mins: u64,
    pub history: u64,
    pub comment: String,
    pub create_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

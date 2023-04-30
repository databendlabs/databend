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

use ctor::ctor;
use regex::Regex;

pub struct ClickHouseFederated {}

#[ctor]
static FORMAT_REGEX: Regex = Regex::new(r".*(?i)FORMAT\s*([[:alpha:]]*)\s*;?$").unwrap();

impl ClickHouseFederated {
    pub fn get_format(query: &str) -> Option<String> {
        match FORMAT_REGEX.captures(query) {
            Some(x) => x.get(1).map(|s| s.as_str().to_owned()),
            None => None,
        }
    }
}

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

use std::collections::BTreeMap;

use lazy_static::lazy_static;
use regex::Regex;
use regex::RegexBuilder;
use serde::Deserialize;
use serde::Serialize;

lazy_static! {
    pub static ref SET_SQL_RE: Regex =
        RegexBuilder::new(r"^SET\s+(?P<key>\w+)\s*=\s*[']?(?P<value>[^;[']]+)[']?\s*;?")
            .case_insensitive(true)
            .build()
            .unwrap();
    pub static ref UNSET_SQL_RE: Regex = RegexBuilder::new(r"^UNSET\s+(?P<key>\w+)\s*;?")
        .case_insensitive(true)
        .build()
        .unwrap();
    pub static ref USE_SQL_RE: Regex = RegexBuilder::new(r"^use\s+(?P<db>\w+)\s*;?")
        .case_insensitive(true)
        .build()
        .unwrap();
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpSessionConf {
    pub database: Option<String>,
    pub keep_server_session_secs: Option<u64>,
    pub settings: Option<BTreeMap<String, String>>,
}

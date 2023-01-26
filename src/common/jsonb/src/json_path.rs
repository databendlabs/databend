// Copyright 2023 Datafuse Labs.
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

use std::borrow::Cow;
use std::fmt::Debug;
use std::string::ToString;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub enum JsonPath {
    String(String),
    UInt64(u64),
}

impl ToString for JsonPath {
    fn to_string(&self) -> String {
        match self {
            JsonPath::String(s) => format!("['{}']", s),
            JsonPath::UInt64(n) => format!("[{}]", n),
        }
    }
}

impl<'a> JsonPath {
    pub fn as_ref(&'a self) -> JsonPathRef<'a> {
        match self {
            JsonPath::String(v) => JsonPathRef::String(Cow::from(v)),
            JsonPath::UInt64(v) => JsonPathRef::UInt64(*v),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonPathRef<'a> {
    String(Cow<'a, str>),
    UInt64(u64),
}

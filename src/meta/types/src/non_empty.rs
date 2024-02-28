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

/// A container contains non-empty &str
#[derive(Clone, Debug, Copy)]
pub struct NonEmptyStr<'a> {
    non_empty: &'a str,
}

impl<'a> NonEmptyStr<'a> {
    pub fn new(s: &'a str) -> Result<Self, &'static str> {
        if s.is_empty() {
            return Err("input str is empty");
        }
        Ok(NonEmptyStr { non_empty: s })
    }

    pub fn get(&self) -> &str {
        self.non_empty
    }
}

/// A container contains non-empty String
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NonEmptyString {
    non_empty: String,
}

impl NonEmptyString {
    pub fn new(s: impl ToString) -> Result<Self, &'static str> {
        let s = s.to_string();
        if s.is_empty() {
            return Err("input is empty");
        }
        Ok(NonEmptyString { non_empty: s })
    }

    pub fn as_str(&self) -> &str {
        &self.non_empty
    }
}

impl ToString for NonEmptyString {
    fn to_string(&self) -> String {
        self.non_empty.clone()
    }
}

impl<'a> From<NonEmptyStr<'a>> for NonEmptyString {
    fn from(value: NonEmptyStr<'a>) -> Self {
        NonEmptyString::new(value.get()).unwrap()
    }
}

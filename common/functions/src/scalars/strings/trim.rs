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
use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone)]
pub struct LTrim;

impl StringOperator for LTrim {
    fn apply(s: &[u8]) -> &[u8] {
        for (idx, ch) in s.iter().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                return &s[idx..];
            }
        }
        b""
    }
}

#[derive(Clone)]
pub struct RTrim;

impl StringOperator for RTrim {
    fn apply(s: &[u8]) -> &[u8] {
        for (idx, ch) in s.iter().rev().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                return &s[..s.len() - idx];
            }
        }
        b""
    }
}

pub type LTrimFunction = String2StringFunction<LTrim>;
pub type RTrimFunction = String2StringFunction<RTrim>;

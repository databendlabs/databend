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

use super::NumberOperator;
use super::String2NumberFunction;

#[derive(Clone, Default)]
pub struct Ord {}

impl NumberOperator<u64> for Ord {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn apply<'a>(&'a mut self, str: &'a [u8]) -> u64 {
        let mut res: u64 = 0;
        if !str.is_empty() {
            if str[0].is_ascii() {
                res = str[0] as u64;
            } else {
                for (p, _) in str.iter().enumerate() {
                    let s = &str[0..p + 1];
                    if std::str::from_utf8(s).is_ok() {
                        for (i, b) in s.iter().rev().enumerate() {
                            res += (*b as u64) * 256_u64.pow(i as u32);
                        }
                        break;
                    }
                }
            }
        }
        res
    }
}

pub type OrdFunction = String2NumberFunction<Ord, u64>;

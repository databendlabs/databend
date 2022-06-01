//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::cmp;

use common_datavalues::DataValue;

pub trait Trim {
    fn trim(self) -> Self;
}

impl Trim for DataValue {
    fn trim(self) -> Self {
        match self {
            DataValue::String(v) => {
                // take at most 5 **bytes, NOT characters** from the underlying `Vec<u8>`
                let l = cmp::min(5, v.len());
                let trimmed = v.as_slice()[..l].to_vec();
                DataValue::String(trimmed)
            }
            other => other,
        }
    }
}

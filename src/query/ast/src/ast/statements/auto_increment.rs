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

use std::ops::Add;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AutoIncrement {
    pub start: u64,
    pub step: u64,
    // Ensure that the generated sequence values are distributed strictly in the order of insertion time
    pub is_ordered: bool,
}

impl AutoIncrement {
    pub fn to_sql_string(&self) -> String {
        let string = format!("AUTOINCREMENT ({}, {}) ", self.start, self.step);
        if self.is_ordered {
            string.add("ORDER")
        } else {
            string.add("NOORDER")
        }
    }
}

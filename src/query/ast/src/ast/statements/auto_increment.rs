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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AutoIncrement {
    pub start_num: u64,
    pub step_num: u64,
    pub is_order: bool,
    pub is_identity: bool,
}

impl AutoIncrement {
    pub fn sequence_name(database: &str, table_id: u64, column_id: u32) -> String {
        format!("_sequence_{database}_{table_id}_{column_id}")
    }
}

impl Display for AutoIncrement {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.is_identity {
            write!(f, "IDENTITY ({}, {})", self.start_num, self.step_num)?;
            if self.is_order {
                write!(f, " ORDER")?;
            } else {
                write!(f, " NOORDER")?;
            }
        } else {
            write!(f, "AUTOINCREMENT")?;
        }
        Ok(())
    }
}

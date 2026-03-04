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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;

use crate::FromToProto;
use crate::Incompatible;

impl FromToProto for DateTime<Utc> {
    type PB = String;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn from_pb(p: String) -> Result<Self, Incompatible> {
        let v = DateTime::<Utc>::from_str(&p)
            .map_err(|e| Incompatible::new(format!("DateTime error: {}", e)))?;
        Ok(v)
    }

    fn to_pb(&self) -> Result<String, Incompatible> {
        let p = self.to_string();
        Ok(p)
    }
}

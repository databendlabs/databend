// Copyright 2020 Datafuse Labs.
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

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// Sequence number that is used in SledTree
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SeqNum(pub u64);

impl std::ops::Add<u64> for SeqNum {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        SeqNum(self.0 + rhs)
    }
}

impl fmt::Display for SeqNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SeqNum> for u64 {
    fn from(sn: SeqNum) -> Self {
        sn.0
    }
}

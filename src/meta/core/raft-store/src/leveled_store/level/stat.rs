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

use std::fmt;

use crate::leveled_store::level_index::LevelIndex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelStat {
    pub(crate) level_index: Option<LevelIndex>,
    pub(crate) user_count: u64,
    pub(crate) expire_count: u64,
}

impl LevelStat {
    pub fn with_index(mut self, index: LevelIndex) -> Self {
        self.level_index = Some(index);
        self
    }
}

impl fmt::Display for LevelStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(idx) = &self.level_index {
            write!(f, "[{}]", idx)?;
        } else {
            write!(f, "[writable]")?;
        }

        write!(
            f,
            "(user={}, expire={})",
            self.user_count, self.expire_count
        )
    }
}

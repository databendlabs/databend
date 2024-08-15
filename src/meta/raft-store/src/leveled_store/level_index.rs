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

/// Represents a unique identifier for a level.
///
/// The magnitude of the index indicates the relative freshness of the level.
#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LevelIndex {
    /// The internal sequence number of the level.
    internal_seq: u64,

    /// A unique number within the process.
    uniq: u64,
}

impl LevelIndex {
    pub fn new(internal_seq: u64, uniq: u64) -> Self {
        Self { internal_seq, uniq }
    }
}

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

//! Defines the action to take if a record already exists when adding a new record.

use databend_common_meta_types::MatchSeq;

/// Action to take if a record already exists when adding a new record.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnExist {
    /// Return `Err` for an existing record.
    Error,
    /// Keep the existing record and return `Ok(existing)`.
    Keep,
    /// Overwrite the existing record and return `Ok(replaced)`.
    Replace,
}

impl From<OnExist> for MatchSeq {
    /// Convert [`OnExist`] to [`MatchSeq`].
    ///
    /// - If `Error`, then to add a record only when it does not exist, i.e., `MatchSeq` is `Exact(0)`.
    /// - If `Keep`, then to add a record only when it does not exist, i.e., `MatchSeq` is `Exact(0)`.
    /// - If `Replace`, then always to add a record, i.e., `MatchSeq` matches any value: `GE(0)`.
    fn from(on_exist: OnExist) -> Self {
        match on_exist {
            OnExist::Error => MatchSeq::Exact(0),
            OnExist::Keep => MatchSeq::Exact(0),
            OnExist::Replace => MatchSeq::GE(0),
        }
    }
}

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

use std::sync::Arc;

use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::level::Level;

/// A container for the leveled map data, including the top mutable level and the immutable data.
#[derive(Debug, Default)]
pub struct LeveledMapData {
    /// The top level is the newest and writable.
    ///
    /// It can only be updated with mvcc::Commit
    pub(crate) writable: Level,

    pub(crate) immutable: Arc<ImmutableData>,
}

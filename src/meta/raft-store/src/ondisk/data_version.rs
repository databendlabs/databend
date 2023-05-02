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

/// Available data versions this program can work upon.
///
/// It is store in a standalone `sled::Tree`. In this tree there are two `DataVersion` record: the current version of the on-disk data, and the version to upgrade to.
/// The `upgrading` is `Some` only when the upgrading progress is shut down before finishing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum DataVersion {
    /// The first version.
    /// The Data is compatible with openraft v07 and v08, using openraft::compat.
    V0,
}

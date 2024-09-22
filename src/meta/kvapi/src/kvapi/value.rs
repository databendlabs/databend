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

use std::fmt::Debug;

use crate::kvapi;

/// A value that can be stored in kvapi::KVApi.
pub trait Value: Debug {
    type KeyType: kvapi::Key;

    /// Return keys this value depends on.
    ///
    /// For example, the name-to-id record `database-name -> a database-id`
    /// depends on the `database-id -> database-meta` record.
    /// Thus `DatabaseId::dependency_keys()` returns itself for further traversing.
    ///
    /// It accepts the key bound to this value as an argument.
    fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String>;
}

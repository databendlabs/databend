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

use std::ops::Deref;

use databend_common_meta_kvapi::kvapi;

/// The identifier of a internal record used in an application upon kvapi::KVApi.
///
/// E.g. TableId, DatabaseId.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(pub u64);

impl Id {
    pub fn new(i: u64) -> Self {
        Id(i)
    }
}

/// Convert primitive u64 to Id.
impl From<u64> for Id {
    fn from(i: u64) -> Self {
        Id(i)
    }
}

/// Use `Id` as if using a `u64`
impl Deref for Id {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl kvapi::Value for Id {
    fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
        []
    }
}

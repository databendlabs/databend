// Copyright 2021 Datafuse Labs.
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

use crate::SledKeySpace;

/// Defines low level storage API
pub trait Store<KV: SledKeySpace> {
    type Error: std::error::Error;

    fn insert(&self, key: &KV::K, value: &KV::V) -> Result<Option<KV::V>, Self::Error>;

    fn get(&self, key: &KV::K) -> Result<Option<KV::V>, Self::Error>;

    fn remove(&self, key: &KV::K) -> Result<Option<KV::V>, Self::Error>;

    fn update_and_fetch<F>(&self, key: &KV::K, f: F) -> Result<Option<KV::V>, Self::Error>
    where F: FnMut(Option<KV::V>) -> Option<KV::V>;
}

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

//! This module re-exports the `Marked` type from the `map_api` crate,
//! setting the meta type to `KVMeta`.

#[cfg(test)]
mod marked_test;

use databend_common_meta_types::KVMeta;

use crate::state_machine::ExpireValue;

pub type SeqMarked<T = Vec<u8>> = map_api::SeqMarked<T>;

pub type MetaValue<T = Vec<u8>> = (Option<KVMeta>, T);

impl From<ExpireValue> for SeqMarked<String> {
    fn from(value: ExpireValue) -> Self {
        SeqMarked::new_normal(value.seq, value.key)
    }
}

// Copyright 2026 Datafuse Labs
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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_cache::Equivalent;
use databend_common_cache::MemSized;

use crate::CacheKind;
use crate::handle::cache_ref::ErasedValue;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct EntryKey {
    pub kind: CacheKind,
    pub key: String,
}

impl Hash for EntryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        KeyRef::new(self.kind, &self.key).hash(state)
    }
}

/// Borrowed form of `EntryKey`, so lookups do not allocate.
#[derive(Clone, Copy)]
pub(crate) struct KeyRef<'a> {
    pub kind: CacheKind,
    pub key: &'a str,
}

impl<'a> KeyRef<'a> {
    pub fn new(kind: CacheKind, key: &'a str) -> Self {
        KeyRef { kind, key }
    }

    pub fn to_owned(self) -> EntryKey {
        EntryKey {
            kind: self.kind,
            key: self.key.to_string(),
        }
    }
}

impl Hash for KeyRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.key.hash(state);
    }
}

impl Equivalent<EntryKey> for KeyRef<'_> {
    fn equivalent(&self, key: &EntryKey) -> bool {
        self.kind == key.kind && self.key == key.key
    }
}

pub(crate) struct Entry {
    pub value: ErasedValue,
    pub bytes: usize,
}

impl Entry {
    /// True when someone outside the cache still holds the value, so evicting it frees nothing.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.value) > 1
    }
}

impl MemSized for Entry {
    fn mem_bytes(&self) -> usize {
        self.bytes
    }
}

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

use std::collections::BTreeMap;

use databend_common_meta_types::SeqV;
use log::debug;
use log::warn;

/// The local data that reflects a range of key-values on remote meta-service.
#[derive(Debug, Clone, Default)]
pub struct CacheData {
    /// The last sequence number ever seen from the meta-service.
    pub last_seq: u64,
    /// The key-value data stored in the cache.
    pub data: BTreeMap<String, SeqV>,
}

impl CacheData {
    /// Process the watch response and update the local cache.
    ///
    /// Returns the new last_seq.
    pub(crate) fn apply_update(
        &mut self,
        key: String,
        prev: Option<SeqV>,
        current: Option<SeqV>,
    ) -> u64 {
        debug!(
            "meta-Cache process update(key: {}, prev: {:?}, current: {:?})",
            key, prev, current
        );
        match (prev, current) {
            (_, Some(entry)) => {
                self.last_seq = entry.seq;
                self.data.insert(key, entry);
            }
            (Some(_entry), None) => {
                self.data.remove(&key);
            }
            (None, None) => {
                warn!("both prev and current are None when Cache processing watch response; Not possible, but ignoring");
            }
        };

        self.last_seq
    }
}

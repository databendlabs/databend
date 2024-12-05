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

use databend_common_meta_types::protobuf::watch_request::FilterType;

use crate::watcher::id::WatcherId;
use crate::watcher::KeyRange;

/// Attributes of a watcher that is interested in kv change events.
#[derive(Clone, Debug)]
pub struct WatchDesc {
    pub watcher_id: WatcherId,

    /// Defines how to filter keys with `key_range`.
    pub interested: FilterType,

    /// The range of key this watcher is interested in.
    pub key_range: KeyRange,
}

impl WatchDesc {
    pub fn new(id: WatcherId, interested: FilterType, key_range: KeyRange) -> Self {
        Self {
            watcher_id: id,
            interested,
            key_range,
        }
    }
}

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

use crate::protobuf as pb;
use crate::protobuf::watch_request::FilterType;
use crate::protobuf::WatchRequest;
use crate::protobuf::WatchResponse;
use crate::Change;
use crate::SeqV;

impl WatchRequest {
    pub fn new(key: String, key_end: Option<String>) -> Self {
        WatchRequest {
            key,
            key_end,
            filter_type: FilterType::All as _,
            initial_flush: false,
        }
    }

    pub fn with_filter(mut self, filter_type: FilterType) -> Self {
        self.filter_type = filter_type as _;
        self
    }
}

impl WatchResponse {
    /// Create a new `WatchResponse` with `key`, `prev` and `current` values.
    pub fn new3(key: String, prev: Option<SeqV>, current: Option<SeqV>) -> Self {
        let ev = pb::Event {
            key,
            prev: prev.map(pb::SeqV::from),
            current: current.map(pb::SeqV::from),
        };

        WatchResponse { event: Some(ev) }
    }

    pub fn new(change: &Change<Vec<u8>, String>) -> Option<Self> {
        let ev = pb::Event {
            key: change.ident.clone()?,
            prev: change.prev.clone().map(pb::SeqV::from),
            current: change.result.clone().map(pb::SeqV::from),
        };

        Some(WatchResponse { event: Some(ev) })
    }
}

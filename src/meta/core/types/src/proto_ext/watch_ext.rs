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

mod event_ext;

use crate::Change;
use crate::SeqV;
use crate::protobuf as pb;
use crate::protobuf::WatchRequest;
use crate::protobuf::WatchResponse;
use crate::protobuf::watch_request::FilterType;

impl WatchRequest {
    pub fn new(key: String, key_end: Option<String>) -> Self {
        WatchRequest {
            key,
            key_end,
            filter_type: FilterType::All as _,
            initial_flush: false,
        }
    }

    /// Create a new `WatchRequest` that watch a dir with a directory prefix.
    ///
    /// Such as `/tenant` or `tenant/abc`.
    /// A slash "/" will be appended to the prefix if it does not end with a slash.
    pub fn new_dir(prefix: impl ToString) -> Self {
        let prefix = prefix.to_string();
        let prefix = prefix.trim_end_matches('/');

        let left = format!("{}/", prefix);
        let right = format!("{}0", prefix);

        Self::new(left, Some(right))
    }

    pub fn with_filter(mut self, filter_type: FilterType) -> Self {
        self.filter_type = filter_type as _;
        self
    }

    pub fn with_initial_flush(mut self, initial_flush: bool) -> Self {
        self.initial_flush = initial_flush;
        self
    }
}

impl WatchResponse {
    /// Create a new `WatchResponse` that marks the end of initialization phase.
    ///
    /// This response is used to signal that the initial-flush phase has completed,
    /// and the watch stream can now proceed with real-time change notifications.
    pub fn new_initialization_complete() -> Self {
        WatchResponse {
            event: None,
            is_initialization: false,
        }
    }

    /// Create a new `WatchResponse` for an initial key-value pair.
    /// This represents the initial state of a key, not a change event.
    ///
    /// The response will have `is_initialization` set to true to indicate
    /// this is part of the initial data load rather than a real-time change.
    pub fn new_initialization_event(key: String, value: SeqV) -> Self {
        let ev = pb::Event {
            key,
            prev: None,
            current: Some(pb::SeqV::from(value)),
        };

        WatchResponse {
            event: Some(ev),
            is_initialization: true,
        }
    }

    /// Create a new `WatchResponse` for a key-value change event.
    pub fn new_change_event(key: String, prev: Option<SeqV>, current: Option<SeqV>) -> Self {
        let ev = pb::Event {
            key,
            prev: prev.map(pb::SeqV::from),
            current: current.map(pb::SeqV::from),
        };

        WatchResponse {
            event: Some(ev),
            is_initialization: false,
        }
    }

    pub fn new(change: &Change<Vec<u8>, String>) -> Option<Self> {
        let ev = pb::Event {
            key: change.ident.clone()?,
            prev: change.prev.clone().map(pb::SeqV::from),
            current: change.result.clone().map(pb::SeqV::from),
        };

        Some(WatchResponse {
            event: Some(ev),
            is_initialization: false,
        })
    }

    /// Check if the response is an empty indicator just to indicating initialization completion.
    pub fn is_initialization_complete_flag(&self) -> bool {
        !self.is_initialization && self.event.is_none()
    }

    pub fn unpack(self) -> Option<(String, Option<SeqV>, Option<SeqV>)> {
        let ev = self.event?;
        let key = ev.key;
        let prev = ev.prev.map(SeqV::from);
        let current = ev.current.map(SeqV::from);

        Some((key, prev, current))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_new_dir() {
        let req = super::WatchRequest::new_dir("/tenant");
        assert_eq!(req.key, "/tenant/");
        assert_eq!(req.key_end, Some("/tenant0".to_string()));

        let req = super::WatchRequest::new_dir("tenant/abc/");
        assert_eq!(req.key, "tenant/abc/");
        assert_eq!(req.key_end, Some("tenant/abc0".to_string()));
    }
}

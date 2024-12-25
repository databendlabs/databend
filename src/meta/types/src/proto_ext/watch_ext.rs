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

use std::collections::Bound;

use crate::protobuf as pb;
use crate::protobuf::watch_request::FilterType;
use crate::protobuf::WatchRequest;
use crate::protobuf::WatchResponse;
use crate::Change;

impl WatchRequest {
    /// Build a key range from a `key` and an optional `key_end`.
    pub fn build_key_range(
        key: &String,
        key_end: &Option<String>,
    ) -> Result<(Bound<String>, Bound<String>), &'static str> {
        let left = Bound::Included(key.clone());

        match key_end {
            Some(key_end) => {
                if key >= key_end {
                    return Err("empty range");
                }
                Ok((left, Bound::Excluded(key_end.to_string())))
            }
            None => Ok((left.clone(), left)),
        }
    }

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

    pub fn key_range(&self) -> Result<(Bound<String>, Bound<String>), &'static str> {
        Self::build_key_range(&self.key, &self.key_end)
    }
}

impl WatchResponse {
    pub fn new(change: &Change<Vec<u8>, String>) -> Option<Self> {
        let ev = pb::Event {
            key: change.ident.clone()?,
            prev: change.prev.clone().map(pb::SeqV::from),
            current: change.result.clone().map(pb::SeqV::from),
        };

        Some(WatchResponse { event: Some(ev) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_build_key_range() -> Result<(), &'static str> {
        let x = WatchRequest::build_key_range(&s("a"), &None)?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Included(s("a"))));

        let x = WatchRequest::build_key_range(&s("a"), &Some(s("b")))?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Excluded(s("b"))));

        let x = WatchRequest::build_key_range(&s("a"), &Some(s("a")));
        assert_eq!(x, Err("empty range"));

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}

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

use std::fmt;

use display_more::DisplayOptionExt;

use crate::protobuf::Event;
use crate::protobuf::WatchResponse;

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}: {} -> {})",
            self.key,
            self.prev.display(),
            self.current.display()
        )
    }
}

impl fmt::Display for WatchResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.event.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::KvMeta;
    use crate::protobuf::SeqV;

    #[test]
    fn test_event_display() {
        let event = Event {
            key: "test_key".to_string(),
            prev: Some(SeqV {
                seq: 1,
                data: "test_prev".as_bytes().to_vec(),
                meta: Some(KvMeta {
                    expire_at: Some(1000),
                }),
            }),
            current: Some(SeqV {
                seq: 2,
                data: "test_current".as_bytes().to_vec(),
                meta: None,
            }),
        };
        assert_eq!(event.to_string(), "(test_key: (seq=1 [expire=1970-01-01T00:16:40.000] [test_prev]) -> (seq=2 [] [test_current]))");
    }

    #[test]
    fn test_watch_response_display() {
        let watch_response = WatchResponse {
            event: Some(Event {
                key: "test_key".to_string(),
                prev: Some(SeqV {
                    seq: 1,
                    data: "test_prev".as_bytes().to_vec(),
                    meta: Some(KvMeta {
                        expire_at: Some(1000),
                    }),
                }),
                current: Some(SeqV {
                    seq: 2,
                    data: "test_current".as_bytes().to_vec(),
                    meta: None,
                }),
            }),
        };
        assert_eq!(watch_response.to_string(), "(test_key: (seq=1 [expire=1970-01-01T00:16:40.000] [test_prev]) -> (seq=2 [] [test_current]))");

        let watch_response = WatchResponse { event: None };
        assert_eq!(watch_response.to_string(), "None");
    }
}

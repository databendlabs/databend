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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::SeqV;
use crate::protobuf as pb;

impl pb::FetchIncreaseU64Response {
    pub fn new(key: impl ToString, before: SeqV<u64>, after: SeqV<u64>) -> Self {
        Self {
            key: key.to_string(),
            before_seq: before.seq,
            before: before.data,
            after_seq: after.seq,
            after: after.data,
        }
    }

    pub fn new_unchanged(key: impl ToString, before: SeqV<u64>) -> Self {
        Self {
            key: key.to_string(),
            before_seq: before.seq,
            before: before.data,
            after_seq: before.seq,
            after: before.data,
        }
    }

    pub fn delta(&self) -> u64 {
        self.after.saturating_sub(self.before)
    }
}

impl Display for pb::FetchIncreaseU64Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FetchIncreaseU64Response{{ key={} before=(seq={} {}), after=(seq={} {}), delta={} }}",
            self.key,
            self.before_seq,
            self.before,
            self.after_seq,
            self.after,
            self.delta()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_increase_u64_response() {
        let resp = pb::FetchIncreaseU64Response::new("foo", SeqV::new(10, 100), SeqV::new(20, 200));

        assert_eq!(resp.delta(), 100);
    }

    #[test]
    fn test_display_fetch_increase_u64_response() {
        let resp = pb::FetchIncreaseU64Response::new("k1", SeqV::new(1, 3), SeqV::new(2, 4));
        assert_eq!(
            resp.to_string(),
            "FetchIncreaseU64Response{ key=k1 before=(seq=1 3), after=(seq=2 4), delta=1 }"
        );
    }

    #[test]
    fn test_fetch_increase_u64_response_new() {
        let resp = pb::FetchIncreaseU64Response::new("k1", SeqV::new(1, 3), SeqV::new(2, 4));
        assert_eq!(resp, pb::FetchIncreaseU64Response {
            key: "k1".to_string(),
            before_seq: 1,
            before: 3,
            after_seq: 2,
            after: 4,
        });
    }

    #[test]
    fn test_fetch_increase_u64_response_new_unchanged() {
        let resp = pb::FetchIncreaseU64Response::new_unchanged("k1", SeqV::new(1, 3));
        assert_eq!(resp, pb::FetchIncreaseU64Response {
            key: "k1".to_string(),
            before_seq: 1,
            before: 3,
            after_seq: 1,
            after: 3,
        });
    }
}

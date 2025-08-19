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

use crate::protobuf as pb;

impl pb::TxnGetResponse {
    pub fn new(key: impl ToString, value: Option<pb::SeqV>) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }
}

impl fmt::Display for pb::TxnGetResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Get-resp: key={}, prev_seq={:?}",
            self.key,
            self.value.as_ref().map(|x| x.seq)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_get_response_display() {
        let resp = pb::TxnGetResponse::new("key1", Some(pb::SeqV::new(1, b"value1".to_vec())));
        assert_eq!(resp.to_string(), "Get-resp: key=key1, prev_seq=Some(1)");
    }
}

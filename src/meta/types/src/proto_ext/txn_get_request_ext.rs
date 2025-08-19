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

impl pb::TxnGetRequest {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }
}

impl fmt::Display for pb::TxnGetRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Get key={}", self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_txn_get_request() {
        let req = pb::TxnGetRequest::new("k1");
        assert_eq!(format!("{}", req), "Get key=k1");
    }
}

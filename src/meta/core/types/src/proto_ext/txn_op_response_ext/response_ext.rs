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

use pb::txn_op_response::Response;

use crate::protobuf as pb;

impl Display for Response {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Response::Get(r) => {
                write!(f, "Get: {}", r)
            }
            Response::Put(r) => {
                write!(f, "Put: {}", r)
            }
            Response::Delete(r) => {
                write!(f, "Delete: {}", r)
            }
            Response::DeleteByPrefix(r) => {
                write!(f, "DeleteByPrefix: {}", r)
            }
            Response::FetchIncreaseU64(r) => {
                write!(f, "FetchIncreaseU64: {}", r)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SeqV;
    #[test]
    fn test_from() {
        let resp = Response::from(pb::FetchIncreaseU64Response::new_unchanged(
            "key",
            SeqV::new(1, 2),
        ));

        assert_eq!(
            resp,
            Response::FetchIncreaseU64(pb::FetchIncreaseU64Response {
                key: "key".to_string(),
                before_seq: 1,
                before: 2,
                after_seq: 1,
                after: 2,
            })
        );
    }
}

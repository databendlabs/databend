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

use pb::txn_op::Request;

use crate::protobuf as pb;

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Request::Get(r) => {
                write!(f, "Get({})", r)
            }
            Request::Put(r) => {
                write!(f, "Put({})", r)
            }
            Request::Delete(r) => {
                write!(f, "Delete({})", r)
            }
            Request::DeleteByPrefix(r) => {
                write!(f, "DeleteByPrefix({})", r)
            }
            Request::FetchIncreaseU64(r) => {
                write!(f, "FetchIncreaseU64({})", r)
            }
            Request::PutSequential(r) => {
                write!(f, "PutSequential({})", r)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_request() {
        assert_eq!(
            format!("{}", Request::Get(pb::TxnGetRequest::default())),
            "Get(Get key=)"
        );
        assert_eq!(
            format!("{}", Request::Put(pb::TxnPutRequest::default())),
            "Put(Put key=)"
        );
        assert_eq!(
            format!("{}", Request::Delete(pb::TxnDeleteRequest::default())),
            "Delete(Delete key=)"
        );
        assert_eq!(
            format!(
                "{}",
                Request::DeleteByPrefix(pb::TxnDeleteByPrefixRequest::default())
            ),
            "DeleteByPrefix(TxnDeleteByPrefixRequest prefix=)"
        );
        assert_eq!(
            format!(
                "{}",
                Request::FetchIncreaseU64(pb::FetchIncreaseU64::default())
            ),
            "FetchIncreaseU64(FetchIncreaseU64 key= max_value=0 delta=0)"
        );
    }
}

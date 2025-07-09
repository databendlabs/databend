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

use display_more::DisplaySliceExt;

use crate::protobuf as pb;

impl pb::TxnReply {
    pub fn new(execution_path: impl ToString) -> Self {
        let execution_path = execution_path.to_string();
        Self {
            success: execution_path != "else",
            responses: vec![],
            execution_path,
        }
    }

    /// Return the index of the branch that was executed in [`pb::TxnRequest::operations`].
    ///
    /// If none of the branches were executed, return `None`,
    /// i.e., the `condition` is met and `if_then` is executed, or `else_then` is executed.
    /// In such case, the caller should then compare `execution_path` against "then" or "else` to determine which branch was executed.
    ///
    /// If there is an error parsing the index, return the original `execution_path`.
    pub fn executed_branch_index(&self) -> Result<Option<usize>, &str> {
        // if self.execution_path is in form "operation:<index>", return the index.
        if let Some(index) = self.execution_path.strip_prefix("operation:") {
            index
                .parse()
                .map(Some)
                .map_err(|_| self.execution_path.as_str())
        } else {
            Ok(None)
        }
    }
}

impl Display for pb::TxnReply {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TxnReply{{ success: {}, responses: {} }}",
            self.success,
            self.responses.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_reply_display() {
        let reply = pb::TxnReply::new("operation:0");
        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: true, responses: [] }"
        );

        let reply = pb::TxnReply::new("else");
        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: false, responses: [] }"
        );

        let reply = pb::TxnReply::new("operation:1");
        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: true, responses: [] }"
        );

        // Test other execution paths
        let reply = pb::TxnReply::new("then");
        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: true, responses: [] }"
        );
    }

    #[test]
    fn test_txn_reply_display_with_multiple_responses() {
        use crate::protobuf as pb_types;
        use crate::protobuf::txn_op_response;

        // Test with multiple mixed responses
        let mut reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![
            pb_types::TxnOpResponse {
                response: Some(txn_op_response::Response::Get(pb_types::TxnGetResponse {
                    key: "key1".to_string(),
                    value: Some(pb_types::SeqV {
                        seq: 1,
                        data: b"value1".to_vec(),
                        meta: None,
                    }),
                })),
            },
            pb_types::TxnOpResponse {
                response: Some(txn_op_response::Response::Put(pb_types::TxnPutResponse {
                    key: "key2".to_string(),
                    prev_value: None,
                    current: Some(pb_types::SeqV {
                        seq: 2,
                        data: b"value2".to_vec(),
                        meta: None,
                    }),
                })),
            },
            pb_types::TxnOpResponse {
                response: Some(txn_op_response::Response::Delete(
                    pb_types::TxnDeleteResponse {
                        key: "key3".to_string(),
                        success: true,
                        prev_value: None,
                    },
                )),
            },
        ];

        let display_str = reply.to_string();
        assert_eq!(display_str, "TxnReply{ success: true, responses: [TxnOpResponse: Get: Get-resp: key=key1, prev_seq=Some(1),TxnOpResponse: Put: Put-resp: key=key2, prev_seq=None, current_seq=2,TxnOpResponse: Delete: Delete-resp: success: true, key=key3, prev_seq=None] }");
    }

    #[test]
    fn test_txn_reply_display_with_empty_responses() {
        let mut reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![];

        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: true, responses: [] }"
        );

        // Test with failed transaction (else path)
        let mut reply = pb::TxnReply::new("else");
        reply.responses = vec![];

        assert_eq!(
            reply.to_string(),
            "TxnReply{ success: false, responses: [] }"
        );
    }

    #[test]
    fn test_executed_branch_index() {
        let reply = pb::TxnReply::new("operation:0");
        assert_eq!(reply.executed_branch_index(), Ok(Some(0)));

        let reply = pb::TxnReply::new("operation:5");
        assert_eq!(reply.executed_branch_index(), Ok(Some(5)));

        let reply = pb::TxnReply::new("else");
        assert_eq!(reply.executed_branch_index(), Ok(None));

        let reply = pb::TxnReply::new("then");
        assert_eq!(reply.executed_branch_index(), Ok(None));

        let reply = pb::TxnReply::new("operation:invalid");
        assert_eq!(reply.executed_branch_index(), Err("operation:invalid"));

        let reply = pb::TxnReply::new("operation:");
        assert_eq!(reply.executed_branch_index(), Err("operation:"));
    }

    #[test]
    fn test_txn_reply_new() {
        // Test success cases (anything except "else")
        let reply = pb::TxnReply::new("operation:0");
        assert!(reply.success);
        assert_eq!(reply.execution_path, "operation:0");
        assert!(reply.responses.is_empty());

        let reply = pb::TxnReply::new("then");
        assert!(reply.success);
        assert_eq!(reply.execution_path, "then");

        // Test failure case ("else")
        let reply = pb::TxnReply::new("else");
        assert!(!reply.success);
        assert_eq!(reply.execution_path, "else");
        assert!(reply.responses.is_empty());
    }
}

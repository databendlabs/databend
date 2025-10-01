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
use map_api::match_seq::MatchSeq;

use crate::protobuf as pb;
use crate::time::flexible_timestamp_to_duration;
use crate::ConditionResult;
use crate::Operation;
use crate::UpsertKV;

impl pb::TxnRequest {
    /// Build a transaction request from an upsert operation.
    pub fn from_upsert(upsert: UpsertKV) -> Self {
        let conditions = match upsert.seq {
            MatchSeq::Any => {
                vec![]
            }
            MatchSeq::Exact(v) => {
                vec![pb::TxnCondition::match_seq(
                    &upsert.key,
                    ConditionResult::Eq,
                    v,
                )]
            }
            MatchSeq::GE(v) => {
                vec![pb::TxnCondition::match_seq(
                    &upsert.key,
                    ConditionResult::Ge,
                    v,
                )]
            }
        };

        let op = match upsert.value {
            Operation::Update(x) => {
                let mut op = pb::TxnOp::put(&upsert.key, x);

                if let Some(meta_spec) = upsert.value_meta {
                    op =
                        op.with_expires_at(meta_spec.expire_at.map(flexible_timestamp_to_duration));
                    op = op.with_ttl(meta_spec.ttl.map(|x| x.to_duration()));
                }

                op
            }
            Operation::Delete => pb::TxnOp::delete(&upsert.key),
            #[allow(deprecated)]
            Operation::AsIs => {
                unreachable!("AsIs should be never used ");
            }
        };

        Self::new(conditions, vec![op]).with_else(vec![pb::TxnOp::get(&upsert.key)])
    }

    /// Push a new conditional operation branch to the transaction.
    ///
    /// A branch is just like a `if (condition1 [and|or] condition2 ... ) { then; return; }` block.
    pub fn push_branch(
        mut self,
        expr: Option<pb::BooleanExpression>,
        ops: impl IntoIterator<Item = crate::TxnOp>,
    ) -> Self {
        self.operations
            .push(pb::ConditionalOperation::new(expr, ops));
        self
    }

    /// Push the old version of `condition` and `if_then` to the transaction.
    ///
    /// It is just like a `if (condition) { then; return; }` block.
    pub fn push_if_then(
        mut self,
        conditions: impl IntoIterator<Item = crate::TxnCondition>,
        ops: impl IntoIterator<Item = crate::TxnOp>,
    ) -> Self {
        assert!(self.condition.is_empty());
        assert!(self.if_then.is_empty());
        self.condition.extend(conditions);
        self.if_then.extend(ops);
        self
    }

    pub fn new(conditions: Vec<crate::TxnCondition>, ops: Vec<crate::TxnOp>) -> Self {
        Self {
            operations: vec![],
            condition: conditions,
            if_then: ops,
            else_then: vec![],
        }
    }

    /// Adds operations to execute when the conditions are not met.
    pub fn with_else(mut self, ops: Vec<crate::TxnOp>) -> Self {
        self.else_then = ops;
        self
    }

    /// Creates a transaction request that performs the specified operations
    /// unconditionally.
    pub fn unconditional(ops: Vec<crate::TxnOp>) -> Self {
        Self {
            operations: vec![],
            condition: vec![],
            if_then: ops,
            else_then: vec![],
        }
    }
}

impl Display for pb::TxnRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnRequest{{",)?;

        for op in self.operations.iter() {
            write!(f, "{{ {} }}, ", op)?;
        }

        write!(f, "if:{} ", self.condition.display_n(10),)?;
        write!(f, "then:{} ", self.if_then.display_n(10),)?;
        write!(f, "else:{}", self.else_then.display_n(10),)?;

        write!(f, "}}",)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Interval;
    use crate::MetaSpec;

    #[test]
    fn test_display_txn_request() {
        let op = pb::ConditionalOperation {
            predicate: Some(pb::BooleanExpression::from_conditions_and([
                pb::TxnCondition::eq_seq("k1", 1),
                pb::TxnCondition::eq_seq("k2", 2),
            ])),
            operations: vec![
                //
                pb::TxnOp::put("k1", b"v1".to_vec()),
                pb::TxnOp::put("k2", b"v2".to_vec()),
            ],
        };

        let req = crate::TxnRequest {
            operations: vec![op],
            condition: vec![
                pb::TxnCondition::eq_seq("k1", 1),
                pb::TxnCondition::eq_seq("k2", 2),
            ],
            if_then: vec![
                pb::TxnOp::put("k1", b"v1".to_vec()),
                pb::TxnOp::put("k2", b"v2".to_vec()),
            ],
            else_then: vec![pb::TxnOp::put("k3", b"v1".to_vec())],
        };

        assert_eq!(
            format!("{}", req),
           "TxnRequest{{ if:(k1 == seq(1) AND k2 == seq(2)) then:[Put(Put key=k1),Put(Put key=k2)] }, if:[k1 == seq(1),k2 == seq(2)] then:[Put(Put key=k1),Put(Put key=k2)] else:[Put(Put key=k3)]}",
        );
    }

    #[test]
    fn test_from_upsert() {
        let test_cases = vec![
            (
                "match_seq_any",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "match_seq_exact",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Exact(42),
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "test_key".to_string(),
                        expected: ConditionResult::Eq as i32,
                        target: Some(pb::txn_condition::Target::Seq(42)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "match_seq_ge",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::GE(10),
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "test_key".to_string(),
                        expected: ConditionResult::Ge as i32,
                        target: Some(pb::txn_condition::Target::Seq(10)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "delete_operation",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Delete,
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest {
                            key: "test_key".to_string(),
                            prev_value: true,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_ttl_and_expire_at",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec {
                        expire_at: Some(1234567890),
                        ttl: Some(Interval::from_secs(3600)),
                    }),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: Some(1_234_567_890_000),
                            ttl_ms: Some(3600 * 1000), // 3600 seconds in milliseconds
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_only_ttl",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec {
                        expire_at: None,
                        ttl: Some(Interval::from_millis(500)),
                    }),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: Some(500),
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_only_expire_at",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec {
                        expire_at: Some(9876543210),
                        ttl: None,
                    }),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: Some(9_876_543_210_000),
                            ttl_ms: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "complex_case_exact_seq_with_delete",
                UpsertKV {
                    key: "complex_key".to_string(),
                    seq: MatchSeq::Exact(100),
                    value: Operation::Delete,
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "complex_key".to_string(),
                        expected: ConditionResult::Eq as i32,
                        target: Some(pb::txn_condition::Target::Seq(100)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest {
                            key: "complex_key".to_string(),
                            prev_value: true,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
        ];

        for (test_name, input_upsert, expected_output) in test_cases {
            let actual_output = pb::TxnRequest::from_upsert(input_upsert);
            assert_eq!(
                actual_output, expected_output,
                "Test case '{}' failed",
                test_name
            );
        }
    }
}

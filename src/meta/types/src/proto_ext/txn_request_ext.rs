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

impl pb::TxnRequest {
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
}

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
use display_more::DisplaySliceExt;

use crate::protobuf as pb;

impl pb::ConditionalOperation {
    pub fn new(
        expr: Option<pb::BooleanExpression>,
        ops: impl IntoIterator<Item = pb::TxnOp>,
    ) -> Self {
        Self {
            predicate: expr,
            operations: ops.into_iter().collect(),
        }
    }
}

impl fmt::Display for pb::ConditionalOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "if:({}) then:{}",
            self.predicate.display(),
            self.operations.display_n(10)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TxnCondition;
    use crate::TxnOp;

    #[test]
    fn test_new_with_predicate_and_operations() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq("key1", 1),
            pb::TxnCondition::eq_seq("key2", 2),
        ]));

        let operations = vec![
            pb::TxnOp::put("key1", b"value1".to_vec()),
            pb::TxnOp::get("key2"),
        ];

        let cond_op = pb::ConditionalOperation::new(predicate.clone(), operations.clone());

        assert_eq!(cond_op.predicate, predicate);
        assert_eq!(cond_op.operations.len(), 2);
        assert_eq!(cond_op.operations, operations);
    }

    #[test]
    fn test_new_with_iterator_from_array() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_value("key", b"value".to_vec()),
        ]));

        let operations = [
            pb::TxnOp::put("key1", b"val1".to_vec()),
            pb::TxnOp::put("key2", b"val2".to_vec()),
            pb::TxnOp::put("key3", b"val3".to_vec()),
        ];

        let cond_op = pb::ConditionalOperation::new(predicate, operations);

        assert_eq!(cond_op.operations.len(), 3);
    }

    #[test]
    fn test_new_with_various_operation_types() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq("counter", 10),
        ]));

        let operations = vec![
            pb::TxnOp::get("key1"),
            pb::TxnOp::put("key2", b"value".to_vec()),
            pb::TxnOp::delete("key3"),
            pb::TxnOp::fetch_add_u64("counter", 1),
        ];

        let cond_op = pb::ConditionalOperation::new(predicate, operations);

        assert_eq!(cond_op.operations.len(), 4);
        // Verify we can access different operation types
        assert!(matches!(
            cond_op.operations[0].request,
            Some(pb::txn_op::Request::Get(_))
        ));
        assert!(matches!(
            cond_op.operations[1].request,
            Some(pb::txn_op::Request::Put(_))
        ));
        assert!(matches!(
            cond_op.operations[2].request,
            Some(pb::txn_op::Request::Delete(_))
        ));
        assert!(matches!(
            cond_op.operations[3].request,
            Some(pb::txn_op::Request::FetchIncreaseU64(_))
        ));
    }

    #[test]
    fn test_display_basic() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq("key1", 1),
        ]));

        let operations = vec![pb::TxnOp::put("key1", b"value1".to_vec())];
        let cond_op = pb::ConditionalOperation::new(predicate, operations);

        let display_str = format!("{}", cond_op);
        assert!(display_str.starts_with("if:("));
        assert!(display_str.contains("key1 == seq(1)"));
        assert!(display_str.contains(") then:["));
        assert!(display_str.contains("Put(Put key=key1)"));
        assert!(display_str.ends_with("]"));
    }

    #[test]
    fn test_display_conditional_operation() {
        let op = crate::protobuf::ConditionalOperation {
            predicate: Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 1),
                TxnCondition::eq_seq("k2", 2),
            ])),
            operations: vec![
                //
                TxnOp::put("k1", b"v1".to_vec()),
                TxnOp::put("k2", b"v2".to_vec()),
            ],
        };

        assert_eq!(
            format!("{}", op),
            "if:(k1 == seq(1) AND k2 == seq(2)) then:[Put(Put key=k1),Put(Put key=k2)]"
        );
    }

    #[test]
    fn test_display_with_many_operations() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq("key", 1),
        ]));

        // Create more than 10 operations to test display_n(10) truncation
        let operations: Vec<_> = (0..5)
            .map(|i| pb::TxnOp::put(format!("key{}", i), format!("value{}", i).into_bytes()))
            .collect();

        let cond_op = pb::ConditionalOperation::new(predicate, operations);

        let display_str = cond_op.to_string();
        assert_eq!(display_str, "if:(key == seq(1)) then:[Put(Put key=key0),Put(Put key=key1),Put(Put key=key2),Put(Put key=key3),Put(Put key=key4)]");
    }

    #[test]
    fn test_new_from_different_iterators() {
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq("key", 1),
        ]));

        // Test with Vec
        let ops_vec = vec![pb::TxnOp::get("key1")];
        let cond_op1 = pb::ConditionalOperation::new(predicate.clone(), ops_vec);
        assert_eq!(cond_op1.operations.len(), 1);

        // Test with array
        let ops_array = [pb::TxnOp::get("key2")];
        let cond_op2 = pb::ConditionalOperation::new(predicate.clone(), ops_array);
        assert_eq!(cond_op2.operations.len(), 1);

        // Test with slice
        let ops_slice = &[pb::TxnOp::get("key3")][..];
        let cond_op3 = pb::ConditionalOperation::new(predicate.clone(), ops_slice.iter().cloned());
        assert_eq!(cond_op3.operations.len(), 1);
    }

    #[test]
    fn test_edge_cases() {
        // Test with very long key names
        let long_key = "a".repeat(1000);
        let predicate = Some(pb::BooleanExpression::from_conditions_and([
            pb::TxnCondition::eq_seq(&long_key, u64::MAX),
        ]));
        let operations = vec![pb::TxnOp::get(&long_key)];

        let cond_op = pb::ConditionalOperation::new(predicate, operations);

        // Verify the long key was stored correctly
        match &cond_op.operations[0].request {
            Some(pb::txn_op::Request::Get(get_req)) => {
                assert_eq!(get_req.key.len(), 1000);
                assert_eq!(get_req.key, long_key);
            }
            _ => panic!("Expected Get request"),
        }

        // Ensure display doesn't panic with long keys
        let _ = format!("{}", cond_op);
    }
}

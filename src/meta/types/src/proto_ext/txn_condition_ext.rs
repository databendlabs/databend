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

mod condition_result_ext;
mod target_ext;

use std::fmt::Display;
use std::fmt::Formatter;

use display_more::DisplayOptionExt;
use num_traits::FromPrimitive;
use pb::txn_condition::ConditionResult;
use pb::txn_condition::Target;

use crate::protobuf as pb;

impl pb::TxnCondition {
    /// Create a txn condition that checks if the `seq` matches.
    pub fn eq_seq(key: impl ToString, seq: u64) -> Self {
        Self::match_seq(key, ConditionResult::Eq, seq)
    }

    /// Create a txn condition that checks if the `seq` match.
    pub fn match_seq(key: impl ToString, op: ConditionResult, seq: u64) -> Self {
        Self {
            key: key.to_string(),
            expected: op as i32,
            target: Some(Target::Seq(seq)),
        }
    }

    pub fn eq_value(key: impl ToString, value: Vec<u8>) -> Self {
        Self::match_value(key, ConditionResult::Eq, value)
    }

    pub fn match_value(key: impl ToString, op: ConditionResult, value: Vec<u8>) -> Self {
        Self {
            key: key.to_string(),
            expected: op as i32,
            target: Some(Target::Value(value)),
        }
    }

    /// Assert that there are exact `n` keys with the given prefix.
    ///
    /// Usually, the prefix should end with a slash `/`.
    pub fn keys_with_prefix(prefix: impl ToString, n: u64) -> Self {
        Self::match_keys_with_prefix(prefix, ConditionResult::Eq, n)
    }

    /// Compare the number of keys with the given prefix against the given `count`.
    ///
    /// Usually, the prefix should end with a slash `/`.
    pub fn match_keys_with_prefix(prefix: impl ToString, op: ConditionResult, count: u64) -> Self {
        Self {
            key: prefix.to_string(),
            expected: op as i32,
            target: Some(Target::KeysWithPrefix(count)),
        }
    }

    pub fn and(self, other: pb::TxnCondition) -> pb::BooleanExpression {
        pb::BooleanExpression::from_conditions_and([self, other])
    }

    pub fn or(self, other: pb::TxnCondition) -> pb::BooleanExpression {
        pb::BooleanExpression::from_conditions_or([self, other])
    }
}

impl Display for pb::TxnCondition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let expect: ConditionResult = FromPrimitive::from_i32(self.expected).unwrap();

        write!(f, "{} {} {}", self.key, expect, self.target.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_seq() {
        let condition = pb::TxnCondition::eq_seq("test_key", 42);

        assert_eq!(condition.key, "test_key");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::Seq(42)));
    }

    #[test]
    fn test_match_seq_eq() {
        let condition = pb::TxnCondition::match_seq("key1", ConditionResult::Eq, 100);

        assert_eq!(condition.key, "key1");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::Seq(100)));
    }

    #[test]
    fn test_match_seq_gt() {
        let condition = pb::TxnCondition::match_seq("key2", ConditionResult::Gt, 50);

        assert_eq!(condition.key, "key2");
        assert_eq!(condition.expected, ConditionResult::Gt as i32);
        assert_eq!(condition.target, Some(Target::Seq(50)));
    }

    #[test]
    fn test_match_seq_lt() {
        let condition = pb::TxnCondition::match_seq("key3", ConditionResult::Lt, 25);

        assert_eq!(condition.key, "key3");
        assert_eq!(condition.expected, ConditionResult::Lt as i32);
        assert_eq!(condition.target, Some(Target::Seq(25)));
    }

    #[test]
    fn test_eq_value() {
        let value = b"test_value".to_vec();
        let condition = pb::TxnCondition::eq_value("value_key", value.clone());

        assert_eq!(condition.key, "value_key");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::Value(value)));
    }

    #[test]
    fn test_match_value_eq() {
        let value = b"match_value".to_vec();
        let condition = pb::TxnCondition::match_value("key", ConditionResult::Eq, value.clone());

        assert_eq!(condition.key, "key");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::Value(value)));
    }

    #[test]
    fn test_match_value_ne() {
        let value = b"different_value".to_vec();
        let condition = pb::TxnCondition::match_value("key", ConditionResult::Ne, value.clone());

        assert_eq!(condition.key, "key");
        assert_eq!(condition.expected, ConditionResult::Ne as i32);
        assert_eq!(condition.target, Some(Target::Value(value)));
    }

    #[test]
    fn test_match_value_empty() {
        let value = Vec::new();
        let condition =
            pb::TxnCondition::match_value("empty_key", ConditionResult::Eq, value.clone());

        assert_eq!(condition.key, "empty_key");
        assert_eq!(condition.target, Some(Target::Value(value)));
    }

    #[test]
    fn test_keys_with_prefix() {
        let condition = pb::TxnCondition::keys_with_prefix("/prefix/", 5);

        assert_eq!(condition.key, "/prefix/");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::KeysWithPrefix(5)));
    }

    #[test]
    fn test_keys_with_prefix_zero_count() {
        let condition = pb::TxnCondition::keys_with_prefix("/empty/", 0);

        assert_eq!(condition.key, "/empty/");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::KeysWithPrefix(0)));
    }

    #[test]
    fn test_match_keys_with_prefix_eq() {
        let condition = pb::TxnCondition::match_keys_with_prefix("/test/", ConditionResult::Eq, 10);

        assert_eq!(condition.key, "/test/");
        assert_eq!(condition.expected, ConditionResult::Eq as i32);
        assert_eq!(condition.target, Some(Target::KeysWithPrefix(10)));
    }

    #[test]
    fn test_match_keys_with_prefix_gt() {
        let condition =
            pb::TxnCondition::match_keys_with_prefix("/prefix/", ConditionResult::Gt, 3);

        assert_eq!(condition.key, "/prefix/");
        assert_eq!(condition.expected, ConditionResult::Gt as i32);
        assert_eq!(condition.target, Some(Target::KeysWithPrefix(3)));
    }

    #[test]
    fn test_match_keys_with_prefix_lt() {
        let condition =
            pb::TxnCondition::match_keys_with_prefix("/limit/", ConditionResult::Lt, 100);

        assert_eq!(condition.key, "/limit/");
        assert_eq!(condition.expected, ConditionResult::Lt as i32);
        assert_eq!(condition.target, Some(Target::KeysWithPrefix(100)));
    }

    #[test]
    fn test_and_combination() {
        let cond1 = pb::TxnCondition::eq_seq("key1", 1);
        let cond2 = pb::TxnCondition::eq_seq("key2", 2);

        let bool_expr = cond1.and(cond2);

        assert_eq!(
            bool_expr.operator(),
            pb::boolean_expression::CombiningOperator::And
        );
        assert_eq!(bool_expr.conditions.len(), 2);
        assert_eq!(bool_expr.sub_expressions.len(), 0);

        // Check the conditions are included
        assert!(bool_expr.conditions.iter().any(|c| c.key == "key1"));
        assert!(bool_expr.conditions.iter().any(|c| c.key == "key2"));
    }

    #[test]
    fn test_or_combination() {
        let cond1 = pb::TxnCondition::eq_value("key1", b"value1".to_vec());
        let cond2 = pb::TxnCondition::keys_with_prefix("/prefix/", 5);

        let bool_expr = cond1.or(cond2);

        assert_eq!(
            bool_expr.operator(),
            pb::boolean_expression::CombiningOperator::Or
        );
        assert_eq!(bool_expr.conditions.len(), 2);
        assert_eq!(bool_expr.sub_expressions.len(), 0);

        // Check the conditions are included
        assert!(bool_expr.conditions.iter().any(|c| c.key == "key1"));
        assert!(bool_expr.conditions.iter().any(|c| c.key == "/prefix/"));
    }

    #[test]
    fn test_string_keys() {
        // Test that different string types work for keys
        let cond1 = pb::TxnCondition::eq_seq("static_str", 1);
        let cond2 = pb::TxnCondition::eq_seq(String::from("owned_string"), 2);
        let key_ref = "string_ref";
        let cond3 = pb::TxnCondition::eq_seq(key_ref, 3);

        assert_eq!(cond1.key, "static_str");
        assert_eq!(cond2.key, "owned_string");
        assert_eq!(cond3.key, "string_ref");
    }

    #[test]
    fn test_method_chaining_and_or() {
        let cond1 = pb::TxnCondition::eq_seq("key1", 1);
        let cond2 = pb::TxnCondition::eq_seq("key2", 2);
        let cond3 = pb::TxnCondition::eq_seq("key3", 3);

        // Test chaining: (cond1 AND cond2) OR cond3
        let bool_expr = cond1.and(cond2).or(cond3);

        assert_eq!(
            bool_expr.operator(),
            pb::boolean_expression::CombiningOperator::Or
        );
        assert_eq!(bool_expr.sub_expressions.len(), 1);
        assert_eq!(bool_expr.conditions.len(), 1);

        // The sub-expression should be an AND of cond1 and cond2
        let sub_expr = &bool_expr.sub_expressions[0];
        assert_eq!(
            sub_expr.operator(),
            pb::boolean_expression::CombiningOperator::And
        );
        assert_eq!(sub_expr.conditions.len(), 2);

        // The direct condition should be cond3
        assert_eq!(bool_expr.conditions[0].key, "key3");
    }

    #[test]
    fn test_display_implementation() {
        // Test that conditions can be displayed (this tests the Display impl)
        let seq_cond = pb::TxnCondition::eq_seq("test_key", 42);
        let value_cond = pb::TxnCondition::eq_value("value_key", b"test".to_vec());
        let prefix_cond = pb::TxnCondition::keys_with_prefix("/prefix/", 10);

        assert_eq!(format!("{}", seq_cond), "test_key == seq(42)");
        assert_eq!(format!("{}", value_cond), "value_key == value(...)");
        assert_eq!(
            format!("{}", prefix_cond),
            "/prefix/ == keys_with_prefix(10)"
        );
    }

    #[test]
    fn test_edge_cases() {
        // Test with very large sequence numbers
        let large_seq_cond = pb::TxnCondition::eq_seq("large", u64::MAX);
        assert_eq!(large_seq_cond.target, Some(Target::Seq(u64::MAX)));

        // Test with empty prefix
        let empty_prefix_cond = pb::TxnCondition::keys_with_prefix("", 0);
        assert_eq!(empty_prefix_cond.key, "");
        assert_eq!(empty_prefix_cond.target, Some(Target::KeysWithPrefix(0)));

        // Test with large count
        let large_count_cond = pb::TxnCondition::keys_with_prefix("/test/", u64::MAX);
        assert_eq!(
            large_count_cond.target,
            Some(Target::KeysWithPrefix(u64::MAX))
        );
    }
}

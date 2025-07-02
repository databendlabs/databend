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

use pb::boolean_expression::CombiningOperator;
use pb::txn_condition::ConditionResult;
use pb::txn_condition::Target;

use crate::protobuf as pb;
use crate::seq_value::SeqV;

#[derive(derive_more::From)]
pub enum ExpressionOrCondition {
    Expression(#[from] pb::BooleanExpression),
    Condition(#[from] pb::TxnCondition),
}

impl pb::BooleanExpression {
    pub fn from_conditions_and(conditions: impl IntoIterator<Item = pb::TxnCondition>) -> Self {
        Self::from_conditions(CombiningOperator::And, conditions)
    }

    pub fn from_conditions_or(conditions: impl IntoIterator<Item = pb::TxnCondition>) -> Self {
        Self::from_conditions(CombiningOperator::Or, conditions)
    }

    fn from_conditions(
        op: CombiningOperator,
        conditions: impl IntoIterator<Item = pb::TxnCondition>,
    ) -> Self {
        Self {
            conditions: conditions.into_iter().collect(),
            operator: op as i32,
            sub_expressions: vec![],
        }
    }

    pub fn and(self, expr_or_condition: impl Into<ExpressionOrCondition>) -> Self {
        self.merge(CombiningOperator::And, expr_or_condition)
    }

    pub fn or(self, expr_or_condition: impl Into<ExpressionOrCondition>) -> Self {
        self.merge(CombiningOperator::Or, expr_or_condition)
    }

    pub fn and_many(self, others: impl IntoIterator<Item = pb::BooleanExpression>) -> Self {
        self.merge_expressions(CombiningOperator::And, others)
    }

    pub fn or_many(self, others: impl IntoIterator<Item = pb::BooleanExpression>) -> Self {
        self.merge_expressions(CombiningOperator::Or, others)
    }

    fn merge(
        self,
        op: CombiningOperator,
        expr_or_condition: impl Into<ExpressionOrCondition>,
    ) -> Self {
        let x = expr_or_condition.into();
        match x {
            ExpressionOrCondition::Expression(expr) => self.merge_expressions(op, [expr]),
            ExpressionOrCondition::Condition(cond) => self.merge_conditions(op, [cond]),
        }
    }

    fn merge_conditions(
        mut self,
        op: CombiningOperator,
        condition: impl IntoIterator<Item = pb::TxnCondition>,
    ) -> Self {
        if self.operator == op as i32 {
            self.conditions.extend(condition);
            self
        } else {
            pb::BooleanExpression {
                operator: op as i32,
                sub_expressions: vec![self],
                conditions: condition.into_iter().collect(),
            }
        }
    }

    fn merge_expressions(
        mut self,
        op: CombiningOperator,
        other: impl IntoIterator<Item = pb::BooleanExpression>,
    ) -> Self {
        if self.operator == op as i32 {
            self.sub_expressions.extend(other);
            self
        } else {
            let mut expressions = vec![self];
            expressions.extend(other);
            Self {
                conditions: vec![],
                operator: op as i32,
                sub_expressions: expressions,
            }
        }
    }
}

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

impl pb::TxnOpResponse {
    /// Create a new `TxnOpResponse` of a `Delete` operation.
    pub fn delete(key: impl ToString, success: bool, prev_value: Option<pb::SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Delete(
                pb::TxnDeleteResponse {
                    key: key.to_string(),
                    success,
                    prev_value,
                },
            )),
        }
    }

    /// Create a new `TxnOpResponse` of a `Put` operation.
    pub fn put(
        key: impl ToString,
        prev_value: Option<pb::SeqV>,
        current: Option<pb::SeqV>,
    ) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Put(pb::TxnPutResponse {
                key: key.to_string(),
                prev_value,
                current,
            })),
        }
    }

    pub fn unchanged_fetch_add_u64(key: impl ToString, seq: u64, value: u64) -> Self {
        Self::fetch_add_u64(key, seq, value, seq, value)
    }

    pub fn fetch_add_u64(
        key: impl ToString,
        before_seq: u64,
        before: u64,
        after_seq: u64,
        after: u64,
    ) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::FetchAddU64(
                pb::FetchAddU64Response {
                    key: key.to_string(),
                    before_seq,
                    before,
                    after_seq,
                    after,
                },
            )),
        }
    }

    pub fn get(key: impl ToString, value: Option<SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Get(pb::TxnGetResponse {
                key: key.to_string(),
                value: value.map(pb::SeqV::from),
            })),
        }
    }

    /// Consumes and returns the response as a `Get` response if it is one.
    pub fn into_get(self) -> Option<pb::TxnGetResponse> {
        match self.response {
            Some(pb::txn_op_response::Response::Get(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn as_get(&self) -> &pb::TxnGetResponse {
        self.try_as_get().unwrap()
    }

    /// Returns the response as a `Get` response if it is one.
    pub fn try_as_get(&self) -> Option<&pb::TxnGetResponse> {
        match &self.response {
            Some(pb::txn_op_response::Response::Get(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn try_as_fetch_add_u64(&self) -> Option<&pb::FetchAddU64Response> {
        match &self.response {
            Some(pb::txn_op_response::Response::FetchAddU64(resp)) => Some(resp),
            _ => None,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use crate::protobuf::BooleanExpression;
    use crate::TxnCondition;

    #[test]
    fn test_bool_expression() {
        use BooleanExpression as Expr;

        let cond = |k: &str, seq| TxnCondition::eq_seq(k, seq);

        // from_conditions_and
        let expr = Expr::from_conditions_and([cond("a", 1), cond("b", 2)]);
        assert_eq!(expr.to_string(), "a == seq(1) AND b == seq(2)");

        // from_conditions_or
        let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]);
        assert_eq!(expr.to_string(), "a == seq(1) OR b == seq(2)");

        // and_condition
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]).and(cond("c", 3));
            assert_eq!(
                expr.to_string(),
                "(a == seq(1) OR b == seq(2)) AND c == seq(3)"
            );
        }
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]).and(cond("e", 5));
            assert_eq!(
                expr.to_string(),
                "(a == seq(1) OR b == seq(2)) AND e == seq(5)"
            );
        }

        // or_condition
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]).or(cond("c", 3));
            assert_eq!(
                expr.to_string(),
                "a == seq(1) OR b == seq(2) OR c == seq(3)"
            );
        }
        {
            let expr = Expr::from_conditions_and([cond("a", 1), cond("b", 2)]).or(cond("e", 5));
            assert_eq!(
                expr.to_string(),
                "(a == seq(1) AND b == seq(2)) OR e == seq(5)"
            );
        }

        // and
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)])
                .and(Expr::from_conditions_or([cond("c", 3), cond("d", 4)]));
            assert_eq!(
                expr.to_string(),
                "(a == seq(1) OR b == seq(2)) AND (c == seq(3) OR d == seq(4))"
            );
        }
        // or
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)])
                .or(Expr::from_conditions_or([cond("c", 3), cond("d", 4)]));
            assert_eq!(
                expr.to_string(),
                "(c == seq(3) OR d == seq(4)) OR a == seq(1) OR b == seq(2)"
            );
        }
        // and_many
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]).and_many([
                Expr::from_conditions_or([cond("c", 3), cond("d", 4)]),
                Expr::from_conditions_or([cond("e", 5), cond("f", 6)]),
            ]);
            assert_eq!(
                expr.to_string(),
                "(a == seq(1) OR b == seq(2)) AND (c == seq(3) OR d == seq(4)) AND (e == seq(5) OR f == seq(6))"
            );
        }
        // or_many
        {
            let expr = Expr::from_conditions_or([cond("a", 1), cond("b", 2)]).or_many([
                Expr::from_conditions_or([cond("c", 3), cond("d", 4)]),
                Expr::from_conditions_or([cond("e", 5), cond("f", 6)]),
            ]);
            assert_eq!(
                expr.to_string(),
                "(c == seq(3) OR d == seq(4)) OR (e == seq(5) OR f == seq(6)) OR a == seq(1) OR b == seq(2)"
            );
        }
        // complex
        {
            let expr = cond("a", 1)
                .or(cond("b", 2))
                .and(cond("c", 3).or(cond("d", 4)))
                .or(cond("e", 5)
                    .or(cond("f", 6))
                    .and(cond("g", 7).or(cond("h", 8))));
            assert_eq!(
                expr.to_string(),
                "((a == seq(1) OR b == seq(2)) AND (c == seq(3) OR d == seq(4))) OR ((e == seq(5) OR f == seq(6)) AND (g == seq(7) OR h == seq(8)))"
            );
        }
    }
}

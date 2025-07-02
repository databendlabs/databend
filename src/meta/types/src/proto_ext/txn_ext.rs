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

use crate::protobuf as pb;

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

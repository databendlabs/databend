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

mod seqv_display;
mod watch_display;

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use display_more::DisplayUnixTimeStampExt;
use num_traits::FromPrimitive;

use crate::protobuf::boolean_expression::CombiningOperator;
use crate::protobuf::BooleanExpression;
use crate::protobuf::ConditionalOperation;
use crate::txn_condition::Target;
use crate::txn_op;
use crate::txn_op::Request;
use crate::txn_op_response::Response;
use crate::ConditionResult;
use crate::TxnCondition;
use crate::TxnDeleteByPrefixRequest;
use crate::TxnDeleteByPrefixResponse;
use crate::TxnDeleteRequest;
use crate::TxnDeleteResponse;
use crate::TxnOpResponse;
use crate::TxnPutRequest;
use crate::TxnPutResponse;

impl Display for TxnCondition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let expect: ConditionResult = FromPrimitive::from_i32(self.expected).unwrap();

        write!(f, "{} {} {}", self.key, expect, self.target.display())
    }
}

impl Display for ConditionResult {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let x = match self {
            ConditionResult::Eq => "==",
            ConditionResult::Gt => ">",
            ConditionResult::Ge => ">=",
            ConditionResult::Lt => "<",
            ConditionResult::Le => "<=",
            ConditionResult::Ne => "!=",
        };
        write!(f, "{}", x)
    }
}

impl Display for txn_op::Request {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
            Request::FetchAddU64(r) => {
                write!(f, "FetchAddU64({})", r)
            }
        }
    }
}

impl Display for TxnPutRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Put key={}", self.key)?;
        if let Some(expire_at) = self.expire_at {
            write!(
                f,
                " expire_at: {}",
                Duration::from_millis(expire_at).display_unix_timestamp()
            )?;
        }
        if let Some(ttl_ms) = self.ttl_ms {
            write!(f, "  ttl: {:?}", Duration::from_millis(ttl_ms))?;
        }
        Ok(())
    }
}

impl Display for TxnDeleteRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Delete key={}", self.key)
    }
}

impl Display for TxnDeleteByPrefixRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnDeleteByPrefixRequest prefix={}", self.prefix)
    }
}

impl Display for Target {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Target::Value(_) => {
                write!(f, "value(...)",)
            }
            Target::Seq(seq) => {
                write!(f, "seq({})", seq)
            }
            Target::KeysWithPrefix(n) => {
                write!(f, "keys_with_prefix({})", n)
            }
        }
    }
}

impl Display for TxnOpResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnOpResponse: {}", self.response.display())
    }
}

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
            Response::FetchAddU64(r) => {
                write!(f, "FetchAddU64: {}", r)
            }
        }
    }
}

impl Display for TxnPutResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Put-resp: key={}, prev_seq={}, current_seq={}",
            self.key,
            self.prev_value.as_ref().map(|x| x.seq).display(),
            self.current.as_ref().map(|x| x.seq).display()
        )
    }
}
impl Display for TxnDeleteResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Delete-resp: success: {}, key={}, prev_seq={:?}",
            self.success,
            self.key,
            self.prev_value.as_ref().map(|x| x.seq)
        )
    }
}

impl Display for TxnDeleteByPrefixResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TxnDeleteByPrefixResponse prefix={},count={}",
            self.prefix, self.count
        )
    }
}

impl Display for BooleanExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = self.operator();
        let op = match op {
            CombiningOperator::And => "AND",
            CombiningOperator::Or => "OR",
        };

        let mut printed = false;

        for expr in self.sub_expressions.iter() {
            if printed {
                write!(f, " {} ", op)?;
            }
            write!(f, "({})", expr)?;
            printed = true;
        }

        for cond in self.conditions.iter() {
            if printed {
                write!(f, " {} ", op)?;
            }
            write!(f, "{}", cond)?;
            printed = true;
        }
        Ok(())
    }
}

impl Display for ConditionalOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
    use crate::protobuf::BooleanExpression;
    use crate::protobuf::TxnCondition;
    use crate::TxnOp;

    #[test]
    fn test_tx_display_with_bool_expression() {
        let expr = BooleanExpression::from_conditions_and([
            TxnCondition::eq_seq("k1", 1),
            TxnCondition::eq_seq("k2", 2),
        ])
        .and(BooleanExpression::from_conditions_or([
            TxnCondition::eq_seq("k3", 3),
            TxnCondition::eq_seq("k4", 4),
            TxnCondition::keys_with_prefix("k5", 10),
        ]));

        assert_eq!(
            format!("{}", expr),
            "(k3 == seq(3) OR k4 == seq(4) OR k5 == keys_with_prefix(10)) AND k1 == seq(1) AND k2 == seq(2)"
        );
    }

    #[test]
    fn test_display_conditional_operation() {
        let op = crate::protobuf::ConditionalOperation {
            predicate: Some(BooleanExpression::from_conditions_and([
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
}

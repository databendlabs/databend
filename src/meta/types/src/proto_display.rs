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
use std::time::Duration;
use std::time::SystemTime;

use num_traits::FromPrimitive;

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
use crate::TxnGetRequest;
use crate::TxnGetResponse;
use crate::TxnOp;
use crate::TxnOpResponse;
use crate::TxnPutRequest;
use crate::TxnPutResponse;
use crate::TxnReply;
use crate::TxnRequest;

struct OptionDisplay<'a, T: Display> {
    t: &'a Option<T>,
}

impl<'a, T: Display> Display for OptionDisplay<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match &self.t {
            None => {
                write!(f, "None")
            }
            Some(x) => x.fmt(f),
        }
    }
}

pub struct VecDisplay<'a, T: Display> {
    at_most: Option<usize>,
    vec: &'a Vec<T>,
}

impl<'a, T> VecDisplay<'a, T>
where T: Display
{
    pub fn new(vec: &'a Vec<T>) -> Self {
        VecDisplay { at_most: None, vec }
    }

    pub fn new_at_most(vec: &'a Vec<T>, at_most: usize) -> Self {
        VecDisplay {
            at_most: Some(at_most),
            vec,
        }
    }
}

impl<'a, T: Display> Display for VecDisplay<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "[")?;

        for (i, t) in self.vec.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }

            if let Some(at_most) = self.at_most {
                if i >= at_most {
                    write!(f, "...")?;
                    break;
                }
            }

            write!(f, "{}", t)?;
        }

        write!(f, "]")
    }
}

impl Display for TxnRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TxnRequest{{ if:{} then:{} else:{} }}",
            VecDisplay::new_at_most(&self.condition, 5),
            VecDisplay::new_at_most(&self.if_then, 5),
            VecDisplay::new_at_most(&self.else_then, 5),
        )
    }
}

impl Display for TxnCondition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let expect: ConditionResult = FromPrimitive::from_i32(self.expected).unwrap();

        write!(f, "{} {} {}", self.key, expect, OptionDisplay {
            t: &self.target
        })
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

impl Display for TxnOp {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", OptionDisplay { t: &self.request })
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
        }
    }
}

impl Display for TxnGetRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Get key={}", self.key)
    }
}

impl Display for TxnPutRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Put key={}", self.key)?;
        if let Some(expire_at) = self.expire_at {
            let t = SystemTime::UNIX_EPOCH + Duration::from_millis(expire_at);
            write!(f, " expire_at: {:?}", t)?;
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
        }
    }
}

impl Display for TxnReply {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TxnReply{{ success: {}, responses: {}, error: {}}}",
            self.success,
            VecDisplay::new_at_most(&self.responses, 5),
            self.error
        )
    }
}

impl Display for TxnOpResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnOpResponse: {}", OptionDisplay { t: &self.response })
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
        }
    }
}

impl Display for TxnGetResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Get-resp: key={}, prev_seq={:?}",
            self.key,
            self.value.as_ref().map(|x| x.seq)
        )
    }
}
impl Display for TxnPutResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Put-resp: key={}, prev_seq={:?}",
            self.key,
            self.prev_value.as_ref().map(|x| x.seq)
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_vec_display() {
        assert_eq!(
            format!("{}", super::VecDisplay::new(&vec![1, 2, 3])),
            "[1,2,3]"
        );

        assert_eq!(
            format!("{}", super::VecDisplay::new_at_most(&vec![1, 2, 3], 3)),
            "[1,2,3]"
        );

        assert_eq!(
            format!("{}", super::VecDisplay::new_at_most(&vec![1, 2, 3, 4], 3)),
            "[1,2,3,...]"
        );

        assert_eq!(
            format!("{}", super::VecDisplay::new_at_most(&vec![1, 2, 3, 4], 0)),
            "[...]"
        );
    }
}

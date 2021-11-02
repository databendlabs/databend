// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;

use crate::SeqV;

pub enum AddResult<T> {
    Ok(SeqV<T>),
    Exists(SeqV<T>),
}

/// `Change` describes a state change, including the states before and after a change.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::From)]
pub struct Change<T>
where T: Clone + PartialEq
{
    pub prev: Option<SeqV<T>>,
    pub result: Option<SeqV<T>>,
}

impl<T> Change<T>
where T: Clone + PartialEq + std::fmt::Debug
{
    pub fn new(prev: Option<SeqV<T>>, result: Option<SeqV<T>>) -> Self {
        Change { prev, result }
    }

    pub fn new_nochange(prev: Option<SeqV<T>>) -> Self {
        Change {
            prev: prev.clone(),
            result: prev,
        }
    }

    /// Maps `Option<SeqV<T>>` to `Option<U>` for `prev` and `result`.
    pub fn map<F, U>(self, f: F) -> (Option<U>, Option<U>)
    where F: Fn(SeqV<T>) -> U {
        (self.prev.map(|x| f(x)), self.result.map(|x| f(x)))
    }

    /// Extract `prev` and `result`.
    pub fn unpack(self) -> (Option<SeqV<T>>, Option<SeqV<T>>) {
        (self.prev, self.result)
    }

    pub fn unwrap(self) -> (SeqV<T>, SeqV<T>) {
        (self.prev.unwrap(), self.result.unwrap())
    }

    /// Extract `prev.seq` and `result.seq`.
    pub fn unpack_seq(self) -> (Option<u64>, Option<u64>) {
        self.map(|x| x.seq)
    }

    /// Extract `prev.data` and `result.data`.
    pub fn unpack_data(self) -> (Option<T>, Option<T>) {
        self.map(|x| x.data)
    }

    pub fn changed(&self) -> bool {
        self.prev != self.result
    }

    pub fn into_add_result(self) -> Result<AddResult<T>, ErrorCode> {
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return if result.is_some() {
                Ok(AddResult::Exists(p))
            } else {
                Err(ErrorCode::UnknownException(format!(
                    "invalid result for add: prev: {:?} result: None",
                    p
                )))
            };
        }

        if let Some(res) = result {
            Ok(AddResult::Ok(res))
        } else {
            Err(ErrorCode::UnknownException(
                "invalid result for add: prev: None result: None".to_string(),
            ))
        }
    }
}

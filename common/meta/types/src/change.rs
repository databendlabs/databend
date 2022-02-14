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

use serde::Deserialize;
use serde::Serialize;

use crate::MetaResultError;
use crate::SeqV;

pub enum OkOrExist<T> {
    Ok(SeqV<T>),
    Exists(SeqV<T>),
}

pub struct AddResult<T, ID> {
    pub id: Option<ID>,
    pub res: OkOrExist<T>,
}

/// `Change` describes a state change, including the states before and after a change.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::From)]
pub struct Change<T, ID = u64>
where
    ID: Clone + PartialEq,
    T: Clone + PartialEq,
{
    /// identity of the resouce that is changed.
    pub ident: Option<ID>,
    pub prev: Option<SeqV<T>>,
    pub result: Option<SeqV<T>>,
}

impl<T, ID> Change<T, ID>
where
    ID: Clone + PartialEq + std::fmt::Debug,
    T: Clone + PartialEq + std::fmt::Debug,
{
    pub fn new(prev: Option<SeqV<T>>, result: Option<SeqV<T>>) -> Self {
        Change {
            ident: None,
            prev,
            result,
        }
    }

    pub fn new_with_id(id: ID, prev: Option<SeqV<T>>, result: Option<SeqV<T>>) -> Self {
        Change {
            ident: Some(id),
            prev,
            result,
        }
    }

    pub fn nochange_with_id(id: ID, prev: Option<SeqV<T>>) -> Self {
        Change {
            ident: Some(id),
            prev: prev.clone(),
            result: prev,
        }
    }

    /// Maps `Option<SeqV<T>>` to `Option<U>` for `prev` and `result`.
    pub fn map<F, U>(self, f: F) -> (Option<U>, Option<U>)
    where F: Fn(SeqV<T>) -> U + Copy {
        (self.prev.map(f), self.result.map(f))
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

    pub fn into_add_result(mut self) -> Result<AddResult<T, ID>, MetaResultError> {
        let id = self.ident.take();
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return if result.is_some() {
                Ok(AddResult {
                    id,
                    res: OkOrExist::Exists(p),
                })
            } else {
                Err(MetaResultError::InvalidAddResult {
                    prev: "Some".to_string(),
                    result: "None".to_string(),
                })
            };
        }

        if let Some(res) = result {
            Ok(AddResult {
                id,
                res: OkOrExist::Ok(res),
            })
        } else {
            Err(MetaResultError::InvalidAddResult {
                prev: "None".to_string(),
                result: "None".to_string(),
            })
        }
    }
}

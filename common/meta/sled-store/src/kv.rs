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

use std::cmp::Ordering;

use serde::Deserialize;
use serde::Serialize;

/// The meta data of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub expire_at: Option<u64>,
}

/// Value of StateMachine generic-kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVValue<T = Vec<u8>> {
    pub meta: Option<KVMeta>,
    pub value: T,
}

impl<T> KVValue<T> {
    pub fn set_meta(mut self, m: Option<KVMeta>) -> KVValue<T> {
        self.meta = m;
        self
    }

    pub fn set_value(mut self, v: T) -> KVValue<T> {
        self.value = v;
        self
    }
}

/// Compare with a timestamp to check if it is expired.
impl<T> PartialEq<u64> for KVValue<T> {
    fn eq(&self, other: &u64) -> bool {
        match self.meta {
            None => false,
            Some(ref m) => match m.expire_at {
                None => false,
                Some(ref exp) => exp == other,
            },
        }
    }
}

/// Compare with a timestamp to check if it is expired.
impl<T> PartialOrd<u64> for KVValue<T> {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        match self.meta {
            None => None,
            Some(ref m) => m.expire_at.as_ref().map(|exp| exp.cmp(other)),
        }
    }
}

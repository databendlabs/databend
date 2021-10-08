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

/// Compare with a timestamp to check if it is expired.
impl PartialEq<u64> for KVValue {
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
impl PartialOrd<u64> for KVValue {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        match self.meta {
            None => None,
            Some(ref m) => m.expire_at.as_ref().map(|exp| exp.cmp(other)),
        }
    }
}

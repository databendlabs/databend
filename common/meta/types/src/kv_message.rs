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

use crate::Change;
use crate::KVMeta;
use crate::MatchSeq;
use crate::Operation;
use crate::SeqV;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetKVReq {
    pub key: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct MGetKVReq {
    pub keys: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ListKVReq {
    pub prefix: String,
}

pub type UpsertKVActionReply = Change<Vec<u8>>;
pub type GetKVActionReply = Option<SeqV<Vec<u8>>>;
pub type MGetKVActionReply = Vec<Option<SeqV<Vec<u8>>>>;
pub type PrefixListReply = Vec<(String, SeqV<Vec<u8>>)>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpsertKVAction {
    pub key: String,
    pub seq: MatchSeq,
    pub value: Operation<Vec<u8>>,
    pub value_meta: Option<KVMeta>,
}

impl UpsertKVAction {
    pub fn new(
        key: &str,
        seq: MatchSeq,
        value: Operation<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Self {
        Self {
            key: key.to_string(),
            seq,
            value,
            value_meta,
        }
    }
}

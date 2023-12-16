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

use databend_common_meta_types::Change;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::UpsertKV;

pub type UpsertKVReq = UpsertKV;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetKVReq {
    pub key: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MGetKVReq {
    pub keys: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListKVReq {
    pub prefix: String,
}

pub type UpsertKVReply = Change<Vec<u8>>;
pub type GetKVReply = Option<SeqV<Vec<u8>>>;
pub type MGetKVReply = Vec<Option<SeqV<Vec<u8>>>>;
pub type ListKVReply = Vec<(String, SeqV<Vec<u8>>)>;

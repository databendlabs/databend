// Copyright 2020 Datafuse Labs.
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
use crate::SeqV;

pub type UpsertKVActionReply = Change<Vec<u8>>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetKVActionReply {
    pub result: Option<SeqV<Vec<u8>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct MGetKVActionReply {
    pub result: Vec<Option<SeqV<Vec<u8>>>>,
}

pub type PrefixListReply = Vec<(String, SeqV<Vec<u8>>)>;

// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

// both id and name will not change after created
// id used to distinguish share with same name (but never the same time)
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct ShareInfo {
    pub id: u64,
    pub name: String,
    pub meta: ShareMeta,
}

impl ShareInfo {
    pub fn new(id: u64, name: &str) -> Self {
        ShareInfo {
            id,
            name: name.to_string(),
            meta: Default::default(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct ShareMeta {
    pub database: Option<String>,
    pub tables: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateShareReq {
    pub if_not_exists: bool,
    pub tenant: String,
    pub share_name: String,
}

impl Display for CreateShareReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_share(if_not_exists={}):{}/{}",
            self.if_not_exists, self.tenant, self.share_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateShareReply {
    pub share_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropShareReq {
    pub if_exists: bool,
    pub tenant: String,
    pub share_name: String,
}

impl Display for DropShareReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "drop_share:{}/{}", self.tenant, self.share_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropShareReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetShareReq {
    pub tenant: String,
    pub share_name: String,
}

impl GetShareReq {
    pub fn new(tenant: &str, share_name: &str) -> Self {
        GetShareReq {
            tenant: tenant.to_string(),
            share_name: share_name.to_string(),
        }
    }
}

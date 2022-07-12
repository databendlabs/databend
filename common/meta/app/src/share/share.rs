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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use enumflags2::bitflags;
use enumflags2::BitFlags;

use crate::schema::DatabaseNameIdent;
use crate::schema::TableNameIdent;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareNameIdent {
    pub tenant: String,
    pub share_name: String,
}

impl Display for ShareNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.share_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReq {
    pub if_not_exists: bool,
    pub share_name: ShareNameIdent,
    pub comment: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReply {
    pub share_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropShareReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropShareReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareId {
    pub share_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObject {
    Database(DatabaseNameIdent),
    Table(TableNameIdent),
}

impl Display for ShareGrantObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ShareGrantObject::Database(db) => {
                write!(f, "db/{}/{}", db.tenant, db.db_name)
            }
            ShareGrantObject::Table(table) => {
                write!(
                    f,
                    "table/{}/{}/{}",
                    table.tenant, table.db_name, table.table_name
                )
            }
        }
    }
}

// see: https://docs.snowflake.com/en/sql-reference/sql/revoke-privilege-share.html
#[bitflags]
#[repr(u64)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum ShareGrantObjectPrivilege {
    // For DATABASE or SCHEMA
    Usage = 1 << 0,
    // For DATABASE
    ReferenceUsage = 1 << 1,
    // For TABLE or VIEW
    Select = 1 << 2,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantEntry {
    object: ShareGrantObject,
    privileges: BitFlags<ShareGrantObjectPrivilege>,
}

impl ShareGrantEntry {
    pub fn new(object: ShareGrantObject, privileges: BitFlags<ShareGrantObjectPrivilege>) -> Self {
        Self { object, privileges }
    }

    pub fn object(&self) -> &ShareGrantObject {
        &self.object
    }

    pub fn privileges(&self) -> &BitFlags<ShareGrantObjectPrivilege> {
        &self.privileges
    }
}

impl Display for ShareGrantEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.object)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ShareMeta {
    pub database: Option<DatabaseNameIdent>,
    pub entries: BTreeMap<String, ShareGrantEntry>,
    pub accounts: Vec<String>,
    pub comment: Option<String>,
    pub share_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

impl ShareMeta {
    pub fn new(share_on: DateTime<Utc>, comment: Option<String>) -> Self {
        ShareMeta {
            share_on,
            comment,
            ..Default::default()
        }
    }
}

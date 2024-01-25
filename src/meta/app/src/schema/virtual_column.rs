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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_types::MetaId;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct VirtualColumnNameIdent {
    pub tenant: String,
    pub table_id: u64,
}

impl VirtualColumnNameIdent {
    pub fn new(tenant: impl Into<String>, table_id: impl Into<u64>) -> VirtualColumnNameIdent {
        VirtualColumnNameIdent {
            tenant: tenant.into(),
            table_id: table_id.into(),
        }
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }
}

impl Display for VirtualColumnNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'/{}", self.tenant, self.table_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct VirtualColumnMeta {
    pub table_id: MetaId,

    pub virtual_columns: Vec<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: Option<DateTime<Utc>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateVirtualColumnReq {
    pub if_not_exists: bool,
    pub name_ident: VirtualColumnNameIdent,
    pub virtual_columns: Vec<String>,
}

impl Display for CreateVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_virtual_column ({:?}) for {}",
            self.virtual_columns, self.name_ident
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateVirtualColumnReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateVirtualColumnReq {
    pub if_exists: bool,
    pub name_ident: VirtualColumnNameIdent,
    pub virtual_columns: Vec<String>,
}

impl Display for UpdateVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "update_virtual_column ({:?}) for {}",
            self.virtual_columns, self.name_ident
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpdateVirtualColumnReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropVirtualColumnReq {
    pub if_exists: bool,
    pub name_ident: VirtualColumnNameIdent,
}

impl Display for DropVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "drop_virtual_column for {}", self.name_ident)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropVirtualColumnReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListVirtualColumnsReq {
    pub tenant: String,
    pub table_id: Option<MetaId>,
}

impl ListVirtualColumnsReq {
    pub fn new(tenant: impl Into<String>, table_id: Option<MetaId>) -> ListVirtualColumnsReq {
        ListVirtualColumnsReq {
            tenant: tenant.into(),
            table_id,
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::VirtualColumnMeta;
    use crate::schema::VirtualColumnNameIdent;
    use crate::schema::PREFIX_VIRTUAL_COLUMN;

    /// <prefix>/<tenant>/<table_id>
    impl kvapi::Key for VirtualColumnNameIdent {
        const PREFIX: &'static str = PREFIX_VIRTUAL_COLUMN;

        type ValueType = VirtualColumnMeta;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .push_u64(self.table_id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            let table_id = p.next_u64()?;
            p.done()?;

            Ok(VirtualColumnNameIdent { tenant, table_id })
        }
    }
}

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

use super::CreateOption;
use crate::schema::virtual_column_ident::VirtualColumnIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct VirtualColumnMeta {
    pub table_id: MetaId,

    pub virtual_columns: Vec<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateVirtualColumnReq {
    pub create_option: CreateOption,
    pub name_ident: VirtualColumnIdent,
    pub virtual_columns: Vec<String>,
}

impl Display for CreateVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "create_virtual_column ({:?}) for {}",
            self.virtual_columns,
            self.name_ident.display()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateVirtualColumnReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateVirtualColumnReq {
    pub if_exists: bool,
    pub name_ident: VirtualColumnIdent,
    pub virtual_columns: Vec<String>,
}

impl Display for UpdateVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "update_virtual_column ({:?}) for {}",
            self.virtual_columns,
            self.name_ident.display()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpdateVirtualColumnReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropVirtualColumnReq {
    pub if_exists: bool,
    pub name_ident: VirtualColumnIdent,
}

impl Display for DropVirtualColumnReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "drop_virtual_column for {}", self.name_ident.display())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropVirtualColumnReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListVirtualColumnsReq {
    pub tenant: Tenant,
    pub table_id: Option<MetaId>,
}

impl ListVirtualColumnsReq {
    pub fn new(tenant: impl ToTenant, table_id: Option<MetaId>) -> ListVirtualColumnsReq {
        ListVirtualColumnsReq {
            tenant: tenant.to_tenant(),
            table_id,
        }
    }
}

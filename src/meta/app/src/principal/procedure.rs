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
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::types::DataType;

use crate::principal::procedure_id_ident::ProcedureIdIdent;
use crate::principal::procedure_name_ident::ProcedureNameIdent;
use crate::schema::CreateOption;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

#[derive(Clone, Debug, PartialEq)]
pub struct ProcedureInfo {
    pub ident: ProcedureIdIdent,
    pub name_ident: ProcedureNameIdent,
    pub meta: ProcedureMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ProcedureIdent {
    pub procedure_id: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcedureMeta {
    pub return_types: Vec<DataType>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub script: String,
    pub comment: String,
    pub procedure_language: String,
}

impl Default for ProcedureMeta {
    fn default() -> Self {
        ProcedureMeta {
            return_types: vec![],
            created_on: Utc::now(),
            updated_on: Utc::now(),
            script: "".to_string(),
            comment: "".to_string(),
            procedure_language: "SQL".to_string(),
        }
    }
}

impl Display for ProcedureMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Lanuage: {:?}, return_type: {:?}, CreatedOn: {:?}, Script: {:?}, Comment: {:?}",
            self.procedure_language, self.return_types, self.created_on, self.script, self.comment
        )
    }
}

/// Save procedure name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct ProcedureIdList {
    pub id_list: Vec<u64>,
}

impl ProcedureIdList {
    pub fn new() -> ProcedureIdList {
        ProcedureIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, table_id: u64) {
        self.id_list.push(table_id);
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&mut self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for ProcedureIdList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DB id list: {:?}", self.id_list)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateProcedureReq {
    pub create_option: CreateOption,
    pub name_ident: ProcedureNameIdent,
    pub meta: ProcedureMeta,
}

impl Display for CreateProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self.create_option {
            CreateOption::Create => write!(
                f,
                "create_procedure:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.procedure_name(),
                self.meta
            ),
            CreateOption::CreateIfNotExists => write!(
                f,
                "create_procedure_if_not_exists:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.procedure_name(),
                self.meta
            ),

            CreateOption::CreateOrReplace => write!(
                f,
                "create_or_replace_procedure:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.procedure_name(),
                self.meta
            ),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateProcedureReply {
    pub procedure_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameProcedureReq {
    pub if_exists: bool,
    pub name_ident: ProcedureNameIdent,
    pub new_procedure_name: String,
}

impl Display for RenameProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "rename_procedure:{}/{}=>{}",
            self.name_ident.tenant_name(),
            self.name_ident.procedure_name(),
            self.new_procedure_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameProcedureReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropProcedureReq {
    pub if_exists: bool,
    pub name_ident: ProcedureNameIdent,
}

impl Display for DropProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "drop_procedure(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.procedure_name(),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropProcedureReply {
    pub procedure_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropProcedureReq {
    pub name_ident: ProcedureNameIdent,
}

impl Display for UndropProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "undrop_procedure:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.procedure_name(),
        )
    }
}

impl UndropProcedureReq {
    pub fn tenant(&self) -> &Tenant {
        self.name_ident.tenant()
    }
    pub fn procedure_name(&self) -> &str {
        self.name_ident.procedure_name()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UndropProcedureReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetProcedureReq {
    pub inner: ProcedureNameIdent,
}

impl Deref for GetProcedureReq {
    type Target = ProcedureNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetProcedureReq {
    pub fn new(tenant: impl ToTenant, procedure_name: impl ToString) -> GetProcedureReq {
        GetProcedureReq {
            inner: ProcedureNameIdent::new(tenant, procedure_name),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetProcedureReply {
    pub id: u64,
    pub index_meta: ProcedureMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ProcedureInfoFilter {
    // include all dropped procedures
    IncludeDropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListProcedureReq {
    pub tenant: Tenant,
    pub filter: Option<ProcedureInfoFilter>,
}

impl ListProcedureReq {
    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }
}

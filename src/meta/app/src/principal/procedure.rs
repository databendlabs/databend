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
use display_more::DisplaySliceExt;

use crate::principal::procedure_id_ident::ProcedureIdIdent;
use crate::principal::procedure_name_ident::ProcedureNameIdent;
use crate::principal::ProcedureIdentity;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

#[derive(Clone, Debug, PartialEq)]
pub struct ProcedureInfo {
    pub ident: ProcedureIdIdent,
    pub name_ident: ProcedureNameIdent,
    pub meta: ProcedureMeta,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProcedureIdent {
    pub procedure_id: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcedureMeta {
    pub return_types: Vec<DataType>,
    pub arg_names: Vec<String>,
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
            arg_names: vec![],
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
            "Lanuage: {:?}, args {} return_type: {}, CreatedOn: {:?}, Script: {}, Comment: {:?}",
            self.procedure_language,
            self.arg_names.display_n(1000),
            self.return_types.display_n(1000),
            self.created_on,
            self.script,
            self.comment
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateProcedureReq {
    pub name_ident: ProcedureNameIdent,
    pub meta: ProcedureMeta,
}

impl Display for CreateProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}/{}={:?}",
            self.name_ident.tenant_name(),
            self.name_ident.procedure_name(),
            self.meta
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameProcedureReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropProcedureReq {
    pub name_ident: ProcedureNameIdent,
}

impl Display for DropProcedureReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "drop_procedure:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.procedure_name(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropProcedureReply {
    pub procedure_id: u64,
}

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
    pub fn new(tenant: impl ToTenant, procedure_name: ProcedureIdentity) -> GetProcedureReq {
        GetProcedureReq {
            inner: ProcedureNameIdent::new(tenant, procedure_name),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetProcedureReply {
    pub id: u64,
    pub procedure_meta: ProcedureMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

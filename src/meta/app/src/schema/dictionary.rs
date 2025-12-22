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

use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;

use super::dictionary_name_ident::DictionaryNameIdent;
use crate::KeyWithTenant;
use crate::schema::DictionaryIdentity;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

/// Represents the metadata of a dictionary within the system.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DictionaryMeta {
    /// The source of the dictionary, which specifies where the dictionary data comes from, like `MySQL`.
    pub source: String,
    /// Specify the configuration related to the data source in the form of key-value pairs.
    /// For example, `host='localhost' user='root' password='1234'`
    pub options: BTreeMap<String, String>,
    /// Schema refers to an external table that corresponds to the dictionary.
    /// This is typically used to understand the layout and types of data within the dictionary.
    pub schema: Arc<TableSchema>,
    /// A set of key-value pairs is used to represent the annotations for each field in the dictionary, the key being column_id.
    /// For example, if we have `id, address` fields, then field_comments could be `[ '1=student's number','2=home address']`
    pub field_comments: BTreeMap<u32, String>,
    /// A list of primary column IDs.
    /// For example, vec![1, 2] indicating the first and second columns are the primary keys.
    pub primary_column_ids: Vec<u32>,
    /// A general comment string that can be used to provide additional notes or information about the dictionary.
    pub comment: String,
    /// The timestamp indicating when the dictionary was created, in Coordinated Universal Time (UTC).
    pub created_on: DateTime<Utc>,
    /// if used in CreateDictionaryReq,
    /// `updated_on` MUST set to None.
    pub updated_on: Option<DateTime<Utc>>,
}

impl Display for DictionaryMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Source: {}={:?}, Schema: {:?}, Primary_Column_Id: {:?}, CreatedOn: {:?}",
            self.source, self.options, self.schema, self.primary_column_ids, self.created_on
        )
    }
}

impl Default for DictionaryMeta {
    fn default() -> Self {
        DictionaryMeta {
            source: "".to_string(),
            options: BTreeMap::new(),
            schema: Arc::new(TableSchema::empty()),
            primary_column_ids: Vec::new(),
            created_on: Utc::now(),
            updated_on: None,
            comment: "".to_string(),
            field_comments: BTreeMap::new(),
        }
    }
}

impl DictionaryMeta {
    pub fn build_sql_connection_url(&self) -> Result<String> {
        let username = self
            .options
            .get("username")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `username`"))?;
        let password = self
            .options
            .get("password")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `password`"))?;
        let host = self
            .options
            .get("host")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
        let port = self
            .options
            .get("port")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
        let db = self
            .options
            .get("db")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `db`"))?;
        Ok(format!(
            "mysql://{}:{}@{}:{}/{}",
            username, password, host, port, db
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDictionaryReq {
    pub dictionary_ident: DictionaryNameIdent,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReply {
    pub dictionary_id: u64,
    pub dictionary_meta: DictionaryMeta,
    /// Any change to a dictionary causes the seq to increment
    pub dictionary_meta_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDictionaryReq {
    pub tenant: Tenant,
    pub db_id: u64,
}

impl ListDictionaryReq {
    pub fn new(tenant: impl ToTenant, db_id: u64) -> ListDictionaryReq {
        ListDictionaryReq {
            tenant: tenant.to_tenant(),
            db_id,
        }
    }

    pub fn db_id(&self) -> u64 {
        self.db_id
    }

    pub fn tenant(&self) -> String {
        self.tenant.tenant_name().to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReq {
    pub dictionary_meta: DictionaryMeta,
    pub dictionary_ident: DictionaryNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDictionaryReq {
    pub name_ident: DictionaryNameIdent,
    pub new_dict_ident: DictionaryIdentity,
}

impl RenameDictionaryReq {
    pub fn tenant(&self) -> &Tenant {
        self.name_ident.tenant()
    }

    pub fn db_id(&self) -> u64 {
        self.name_ident.db_id()
    }

    pub fn dictionary_name(&self) -> String {
        self.name_ident.dict_name().clone()
    }

    pub fn new_db_id(&self) -> u64 {
        self.new_dict_ident.db_id
    }

    pub fn new_dictionary_name(&self) -> String {
        self.new_dict_ident.dict_name.clone()
    }
}

impl Display for RenameDictionaryReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "rename_dictionary:{}/{}-{}=>{}-{}",
            self.tenant().tenant_name(),
            self.db_id(),
            self.dictionary_name(),
            self.new_db_id(),
            self.new_dictionary_name(),
        )
    }
}

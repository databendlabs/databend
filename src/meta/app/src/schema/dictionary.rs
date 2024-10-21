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
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

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

    pub fn build_redis_connection_url(&self) -> Result<String> {
        let host = self
            .options
            .get("host")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
        let port = self
            .options
            .get("port")
            .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
        Ok(format!("tcp://{}:{}", host, port))
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
    pub if_exists: bool,
    pub name_ident: DictionaryNameIdent,
    pub new_db_name: String,
    pub new_dictionary_name: String,
}

impl RenameDictionaryReq {
    pub fn tenant(&self) -> &Tenant {
        &self.name_ident.tenant()
    }
    pub fn db_id(&self) -> u64 {
        self.name_ident.db_id()
    }
    pub fn dictionary_name(&self) -> String {
        self.name_ident.dict_name().clone()
    }
}

impl Display for RenameDictionaryReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // How to get db_name by db_id
        write!(
            f,
            "rename_dictionary:{}/{}-{}=>{}-{}",
            self.tenant().tenant_name(),
            self.db_id(),
            self.dictionary_name(),
            self.new_db_name,
            self.new_dictionary_name,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDictionaryReply {
    pub dictionary_id: u64,
}

/// Save table name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DictionaryIdList {
    pub id_list: Vec<u64>,
}

impl DictionaryIdList {
    pub fn new() -> DictionaryIdList {
        DictionaryIdList::default()
    }

    pub fn new_with_ids(ids: impl IntoIterator<Item = u64>) -> DictionaryIdList {
        DictionaryIdList {
            id_list: ids.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, dict_id: u64) {
        self.id_list.push(dict_id)
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for DictionaryIdList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DB.Dictionary id list: {:?}", self.id_list)
    }
}

/// The meta-service key for storing dictionary id history ever used by a dictionary name
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DictionaryIdHistoryIdent {
    pub database_id: u64,
    pub dictionary_name: String,
}

impl Display for DictionaryIdHistoryIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'.'{}'", self.database_id, self.dictionary_name)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DictionaryIdToName {
    pub dict_id: u64,
}

impl Display for DictionaryIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DictionaryIdToName{{{}}}", self.dict_id)
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use super::DictionaryIdHistoryIdent;
    use super::DictionaryIdList;
    use super::DictionaryIdToName;
    use crate::schema::dictionary_name_ident::DictionaryNameIdent;
    use crate::schema::DatabaseId;

    /// "_fd_dictionary_id_list/<db_id>/<dict_name> -> id_list"
    impl kvapi::Key for DictionaryIdHistoryIdent {
        const PREFIX: &'static str = "__fd_dictionary_id_list";

        type ValueType = DictionaryIdList;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.database_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for DictionaryIdHistoryIdent {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.database_id).push_str(&self.dictionary_name)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let db_id = parser.next_u64()?;
            let dictionary_name = parser.next_str()?;
            Ok(Self {
                database_id: db_id,
                dictionary_name,
            })
        }
    }

    impl kvapi::Value for DictionaryIdList {
        type KeyType = DictionaryIdHistoryIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            self.id_list.iter().map(|id| id.to_string())
        }
    }

    impl kvapi::KeyCodec for DictionaryIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.dict_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let dict_id = parser.next_u64()?;
            Ok(Self { dict_id })
        }
    }

    /// "__fd_dict_id_to_name/<dict_id> -> DictionaryNameIdent"
    impl kvapi::Key for DictionaryIdToName {
        const PREFIX: &'static str = "__fd_dictionary_id_to_name";

        type ValueType = DictionaryNameIdent;

        fn parent(&self) -> Option<String> {
            Some(self.dict_id.to_string())
        }
    }
}

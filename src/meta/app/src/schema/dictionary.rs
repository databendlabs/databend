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
use databend_common_expression::TableSchema;

use super::tenant_dictionary_ident::TenantDictionaryIdent;
use super::CreateOption;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDictionaryReq {
    pub dictionary_ident: TenantDictionaryIdent,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReply {}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DictionaryIdent {
    pub db_id: u64,
    pub dictionary_name: String,
}

impl Display for DictionaryIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.dictionary_name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReply {
    pub dictionary_id: u64,
    pub dictionary_meta: DictionaryMeta,
    /// Any change to a dictionary causes the seq to increment
    pub dictionary_meta_seq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DictionaryId {
    pub dictionary_id: u64,
}

impl DictionaryId {
    pub fn new(dictionary_id: u64) -> DictionaryId {
        DictionaryId { dictionary_id }
    }
}

impl Display for DictionaryId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DictionaryId{{{}}}", self.dictionary_id)
    }
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReq {
    pub dict_id: u64,
    pub dict_name: String,
    pub dict_meta: DictionaryMeta,
    pub dictionary_ident: TenantDictionaryIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReply {
    pub dictionary_id: u64,
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use super::DictionaryId;
    use super::DictionaryIdent;
    use super::DictionaryMeta;
    use crate::schema::DatabaseId;

    impl kvapi::KeyCodec for DictionaryId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.dictionary_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let dict_id = parser.next_u64()?;
            Ok(Self {
                dictionary_id: dict_id,
            })
        }
    }

    /// "<prefix>/<dictionary_id>"
    impl kvapi::Key for DictionaryId {
        const PREFIX: &'static str = "__fd_dictionary_by_id";

        type ValueType = DictionaryMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for DictionaryIdent {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id).push_str(&self.dictionary_name)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let db_id = parser.next_u64()?;
            let dictionary_name = parser.next_str()?;
            Ok(Self {
                db_id,
                dictionary_name,
            })
        }
    }

    impl kvapi::Key for DictionaryIdent {
        const PREFIX: &'static str = "__fd_dictionary";

        type ValueType = DictionaryId;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::Value for DictionaryIdent {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for DictionaryMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

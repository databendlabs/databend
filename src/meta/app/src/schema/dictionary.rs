// Copyright 2021 Datafuse Labs.
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
use std::fmt::Formatter;
use std::sync::Arc;
use std::fmt::Display;
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::TableSchema;

use crate::tenant::Tenant;
use crate:: tenant::ToTenant;

use super::database_name_ident::DatabaseNameIdent;
use super::CreateOption;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DictionaryMeta {
    pub name: String,
    pub source: String,
    pub options: BTreeMap<String, String>,
    pub schema: Arc<TableSchema>,
    pub primary_column_ids: Vec<u64>,
    pub comment: String,
    pub created_on: DateTime<Utc>,
    pub dropped_on: Option<DateTime<Utc>>,
    pub updated_on: Option<DateTime<Utc>>,
}

impl Display for DictionaryMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Source: {}={:?}, Schema: {:?}, Primary_Column_Id: {:?}, CreatedOn: {:?}",
            self.source,
            self.options,
            self.schema,
            self.primary_column_ids,
            self.created_on
        )
    }
}

impl Default for DictionaryMeta {
    fn default() -> Self {
        DictionaryMeta {
            name: "".to_string(),
            source: "".to_string(),
            options: BTreeMap::new(),
            schema: Arc::new(TableSchema::empty()),
            primary_column_ids: Vec::new(),
            created_on: Utc::now(),
            dropped_on: None,
            updated_on: None,
            comment: "".to_string(),
            
        }
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DictionaryIdent {
    pub dict_id: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDictionaryReq {
    pub create_option: CreateOption,
    pub name_ident: DictionaryNameIdent,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DictionaryNameIdent {
    pub tenant: Tenant,
    pub db_name: String,
    pub dictionary_name: String,
}

impl DictionaryNameIdent {
    pub fn new(
        tenant: impl ToTenant,
        db_name: impl ToString, 
        dictionary_name: impl ToString,
    ) -> DictionaryNameIdent {
        DictionaryNameIdent {
            tenant: tenant.to_tenant(),
            db_name: db_name.to_string(),
            dictionary_name: dictionary_name.to_string(),
        }
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    pub fn dictionary_name(&self) -> String {
        self.dictionary_name.clone()
    }

    pub fn db_name_ident(&self) -> DatabaseNameIdent {
        DatabaseNameIdent::new(&self.tenant, &self.db_name)
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant.tenant_name()
    }

    pub fn new_generic(tenant: impl ToTenant, dictionary_name: impl ToString, db_name: impl ToString) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            dictionary_name: dictionary_name.to_string(),
            db_name: db_name.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DBIdDictionaryNameIdent {
    pub tenant: Tenant,
    pub db_id: u64,
    pub dict_name: String,
}

impl DBIdDictionaryNameIdent {
    pub fn new(
        tenant: impl ToTenant,
        db_id: u64, 
        dict_name: impl ToString,
    ) -> DBIdDictionaryNameIdent {
        DBIdDictionaryNameIdent {
            tenant: tenant.to_tenant(),
            db_id,
            dict_name: dict_name.to_string(),
        }
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    pub fn dictionary_name(&self) -> String {
        self.dictionary_name.clone()
    }

    pub fn db_name_ident(&self) -> DatabaseNameIdent {
        DatabaseNameIdent::new(&self.tenant, &self.db_name)
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant.tenant_name()
    }

    pub fn new_generic(tenant: impl ToTenant, dict_name: impl ToString, db_id: u64) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            dict_name: dict_name.to_string(),
            db_id
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReq {
    pub if_exists: bool,

    pub name_ident: DictionaryNameIdent,
}

impl DropDictionaryReq {
    pub fn dict_name(&self) -> String {
        self.name_ident.dictionary_name.clone()
    }
}

impl Display for DropDictionaryReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "drop_dictionary(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.dictionary_name(),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReply {
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReq {
    pub name_ident: DictionaryNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReply {
    pub dictionary_id: u64,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DBIdDictionaryName {
    pub db_id: u64,
    pub dictionary_name: String,
}

impl Display for DBIdDictionaryName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.dictionary_name)
    }
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

/// The meta-service key for storing dictionary id history ever used by a dictionary name
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DictionaryIdHistoryIdent {
    pub database_id: u64,
    pub dictionary_name: String,
}

impl Display for DictionaryIdHistoryIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.database_id, self.dictionary_name)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DictionaryIdToName {
    pub dictionary_id: u64,
}

impl Display for DictionaryIdToName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DictionaryIdToName{{{}}}", self.dictionary_id)
    }
}

// Save dictionary name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DictionaryIdList {
    pub id_list: Vec<u64>,
}

impl DictionaryIdList {
    pub fn new() -> DictionaryIdList {
        DictionaryIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, dictionary_id: u64) {
        self.id_list.push(dictionary_id);
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

impl Display for DictionaryIdList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DB.Dictionary id list: {:?}", self.id_list)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDictionaryReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for ListDictionaryReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ListDictionaryReq {
    pub fn new(tenant: &Tenant, db_name: impl ToString) -> ListDictionaryReq {
        ListDictionaryReq {
            inner: DatabaseNameIdent::new(tenant, db_name),
        }
    }
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;
    use databend_common_meta_kvapi::kvapi::KeyBuilder;
    use databend_common_meta_kvapi::kvapi::KeyCodec;
    use databend_common_meta_kvapi::kvapi::KeyParser;

    use crate::schema::DatabaseId;
    use crate::schema::DictionaryIdHistoryIdent;
    use crate::tenant::Tenant;

    use super::DBIdDictionaryName;
    use super::DBIdDictionaryNameIdent;
    use super::DictionaryId;
    use super::DictionaryIdToName;
    use super::DictionaryMeta;
    use super::DictionaryNameIdent;

    impl kvapi::KeyCodec for DictionaryIdHistoryIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.database_id).push_str(&self.dictionary_name)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = b.next_u64()?;
            let dictionary_name = b.next_str()?;
            Ok(Self {
                database_id: db_id,
                dictionary_name,
            })
        }
    }

    impl KeyCodec for DictionaryNameIdent {
        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let tenant_name = parser.next_nonempty()?;
            let name = String::decode_key(parser)?;
            
            Ok(DictionaryNameIdent::new_generic(
                Tenant::new_nonempty(tenant_name),
                name,"db_name"
            ))
        }
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            let b = b.push_str(self.tenant_name());
            self.dictionary_name.encode_key(b)
        }
    }

    impl DictionaryNameIdent {// Self = KeyCode + Debug
        pub fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, "__fd_dictionary")?;
            let k = Self::decode_key(&mut p)?;
            p.done()?;
            Ok(k)
        }
    }

    impl kvapi::KeyCodec for DictionaryId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.dictionary_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let dict_id = parser.next_u64()?;
            Ok(Self { dictionary_id: dict_id })
        }
    }

    impl kvapi::Key for DictionaryId {
        const PREFIX: &'static str = "__fd_dictionary_by_id";

        type ValueType = DictionaryMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::Value for DictionaryMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::KeyCodec for DictionaryNameIdent {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            self.dictionary_name.encode_key(b.push_str(self.tenant.tenant_name()).push_str(&self.db_name))
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let tenant_name = parser.next_nonempty()?;
            let name = String::decode_key(parser)?;
            Ok(DictionaryNameIdent::new_generic(Tenant::new_nonempty(tenant_name), name, "my_db".to_string()))
        }
    }

    impl kvapi::Key for DictionaryNameIdent {
        const PREFIX: &'static str = "__fd_dictionary_by_id";
        type ValueType = String;
        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }
    }

    impl kvapi::KeyCodec for DBIdDictionaryName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id).push_str(&self.dictionary_name)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let db_id = parser.next_u64()?;
            let dictionary_name = parser.next_str()?;
            Ok(Self { db_id, dictionary_name })
        }
    }

    impl kvapi::Key for DBIdDictionaryName {
        const PREFIX: &'static str = "__fd_dictionary";

        type ValueType = DictionaryId;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for DictionaryIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(&self.dictionary_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let dictionary_id = parser.next_u64()?;
            Ok(Self { dictionary_id })
        }
    }

    impl kvapi::Key for DictionaryIdToName {
        const PREFIX: &'static str = "__fd_dictionary_id_to_name";
        type ValueType = DBIdDictionaryName;
        fn parent(&self) -> Option<String> {
            Some(DictionaryId::new(self.dictionary_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for DBIdDictionaryNameIdent {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            self.dict_name.encode_key(b.push_str(self.tenant.tenant_name()).push_str(self.db_id))
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
            where Self: Sized {
            let tenant_name = parser.next_nonempty()?;
            let name = String::decode_key(parser)?;
            Ok(DBIdDictionaryNameIdent::new_generic(Tenant::new_nonempty(tenant_name), name, 0))
        }
    }

    impl kvapi::Key for DictionaryNameIdent {
        const PREFIX: &'static str = "__fd_dictionary_by_id";
        type ValueType = String;
        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }
    }
}
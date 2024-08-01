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
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::ffi::os_str::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::TableSchema;

use crate::share::ShareSpec;
use crate::tenant::Tenant;
use crate:: tenant::ToTenant;

use super::database_name_ident::DatabaseNameIdent;
use super::DatabaseType;
use super::CatalogInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DictionaryMeta {
    pub name: String,
    pub source: String,
    pub options: BTreeMap<String, String>,
    pub schema: Arc<TableSchema>,
    pub primary_column_ids: BTreeSet<u64>,
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
            self.primary_column_id,
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
            primary_column_ids: BTreeSet::new(),
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
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryByIdReq {
    pub if_exists: bool,

    pub tenant: Tenant,

    pub dict_id: MetaId,

    pub dictionary_name: String,

    pub db_id: MetaId,
}

impl DropDictionaryByIdReq {
    pub fn dict_id(&self) -> MetaId {
        self.dict_id
    }
}

impl Display for DropDictionaryByIdReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "drop_dictionary_by_id(if_exists={}):{}",
            self.if_exists,
            self.dict_id(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReply {
    //db id, share spec vector
    pub spec_vec: Option<(u64, Vec<ShareSpec>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReq {
    pub inner: DictionaryNameIdent,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyProto {}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;
    use databend_common_meta_kvapi::kvapi::KeyBuilder;
    use databend_common_meta_kvapi::kvapi::KeyError;
    use databend_common_meta_kvapi::kvapi::KeyParser;

    use crate::schema::DBIdDictionaryName;
    use crate::schema::DatabaseId;
    use crate::schema::LeastVisibleTime;
    use crate::schema::LeastVisibleTimeKey;
    use crate::schema::DictionaryId;
    use crate::schema::DictionaryIdHistoryIdent;
    use crate::schema::DictionaryIdList;
    use crate::schema::DictionaryIdToName;
    use crate::schema::DictionaryMeta;

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

    /// "_fd_dictionary_id_list/<db_id>/<dict_name> -> id_list"
    impl kvapi::Key for DictionaryIdHistoryIdent {
        const PREFIX: &'static str = "__fd_dictionary_id_list";

        type ValueType = DictionaryIdList;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.database_id).to_string_key())
        }
    }


}
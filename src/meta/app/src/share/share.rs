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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::Result;
use enumflags2::bitflags;
use enumflags2::BitFlags;

use crate::app_error::AppError;
use crate::app_error::WrongShareObject;
use crate::schema::CreateOption;
use crate::schema::DatabaseMeta;
use crate::schema::TableInfo;
use crate::schema::TableMeta;
use crate::share::share_name_ident::ShareNameIdent;
use crate::share::share_name_ident::ShareNameIdentRaw;
use crate::share::ShareEndpointIdent;
use crate::storage::mask_string;
use crate::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesReq {
    pub tenant: Tenant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareAccountReply {
    pub share_name: ShareNameIdent,
    pub database_name: Option<String>,
    // for outbound share account, it is the time share has been created.
    // for inbound share account, it is the time accounts has been added to the share.
    pub create_on: DateTime<Utc>,
    // if is inbound share, then accounts is None
    pub accounts: Option<Vec<String>>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesReply {
    // sharing to other accounts(outbound shares)
    pub outbound_accounts: Vec<ShareAccountReply>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReq {
    pub if_not_exists: bool,
    pub share_name: ShareNameIdent,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReply {
    pub share_id: u64,

    pub spec_vec: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropShareReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropShareReply {
    pub share_id: Option<u64>,
    pub spec_vec: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddShareAccountsReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
    pub accounts: Vec<String>,
    pub share_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AddShareAccountsReply {
    pub share_id: Option<u64>,
    pub spec_vec: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveShareAccountsReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
    pub accounts: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RemoveShareAccountsReply {
    pub share_id: Option<u64>,
    pub spec_vec: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowShareOfReq {
    pub share_name: ShareNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShowShareOfReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObjectName {
    // database name
    Database(String),
    // database name, table name
    Table(String, String),
}

impl Display for ShareGrantObjectName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareGrantObjectName::Database(db) => {
                write!(f, "DATABASE {}", db)
            }
            ShareGrantObjectName::Table(db, table) => {
                write!(f, "TABLE {}.{}", db, table)
            }
        }
    }
}

impl From<databend_common_ast::ast::ShareGrantObjectName> for ShareGrantObjectName {
    fn from(obj: databend_common_ast::ast::ShareGrantObjectName) -> Self {
        match obj {
            databend_common_ast::ast::ShareGrantObjectName::Database(db_name) => {
                ShareGrantObjectName::Database(db_name.name)
            }
            databend_common_ast::ast::ShareGrantObjectName::Table(db_name, table_name) => {
                ShareGrantObjectName::Table(db_name.name, table_name.name)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObjectSeqAndId {
    // db_meta_seq, db_id, DatabaseMeta
    Database(u64, u64, DatabaseMeta),
    // db_id, table_meta_seq, table_id, table_meta
    Table(u64, u64, u64, TableMeta),
}

// share name and shared (table name, table info) map
pub type TableInfoMap = BTreeMap<String, TableInfo>;
pub type ShareTableInfoMap = (String, Option<TableInfoMap>);

// Vec<share name> and table info
pub type ShareVecTableInfo = (Vec<String>, TableInfo);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantShareObjectReq {
    pub share_name: ShareNameIdent,
    pub object: ShareGrantObjectName,
    pub grant_on: DateTime<Utc>,
    pub privilege: ShareGrantObjectPrivilege,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantShareObjectReply {
    pub share_id: u64,
    pub share_spec: Option<ShareSpec>,
    pub grant_share_table: Option<TableInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeShareObjectReq {
    pub share_name: ShareNameIdent,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
    pub update_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevokeShareObjectReply {
    pub share_id: u64,
    pub share_spec: Option<ShareSpec>,
    pub revoke_share_table: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantObjectReq {
    pub share_name: ShareNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantReplyObject {
    pub object: ShareGrantObjectName,
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub grant_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantObjectReply {
    pub share_name: ShareNameIdent,
    pub objects: Vec<ShareGrantReplyObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenantsReq {
    pub share_name: ShareNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenants {
    pub account: String,
    pub grant_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenantsReply {
    pub accounts: Vec<GetShareGrantTenants>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetObjectGrantPrivilegesReq {
    pub tenant: Tenant,
    pub object: ShareGrantObjectName,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectGrantPrivilege {
    pub share_name: String,
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub grant_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetObjectGrantPrivilegesReply {
    pub privileges: Vec<ObjectGrantPrivilege>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareCredential {
    HMAC(ShareCredentialHmac),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareCredentialHmac {
    pub key: String,
}

impl Display for &ShareCredential {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareCredential::HMAC(hmac) => {
                write!(f, "{{TYPE:'HMAC',KEY:'{}'}}", mask_string(&hmac.key, 2))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateShareEndpointReq {
    pub create_option: CreateOption,
    pub endpoint: ShareEndpointIdent,
    pub url: String,
    pub credential: Option<ShareCredential>,
    pub args: BTreeMap<String, String>,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateShareEndpointReply {
    pub share_endpoint_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertShareEndpointReq {
    pub endpoint: ShareEndpointIdent,
    pub url: String,
    pub credential: Option<ShareCredential>,
    pub args: BTreeMap<String, String>,
    pub create_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertShareEndpointReply {
    pub share_endpoint_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareEndpointReq {
    pub tenant: Tenant,
    // If `endpoint` is not None, return the specified endpoint,
    // else return all share endpoints meta of tenant
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetShareEndpointReply {
    pub share_endpoint_meta_vec: Vec<(ShareEndpointIdent, ShareEndpointMeta)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropShareEndpointReq {
    pub if_exists: bool,
    pub endpoint: ShareEndpointIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropShareEndpointReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ShareEndpointMeta {
    pub url: String,
    // `tenant` is needless anymore, keep it as empty string
    pub tenant: String,
    pub args: BTreeMap<String, String>,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
    pub credential: Option<ShareCredential>,
}

impl ShareEndpointMeta {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn new(req: &CreateShareEndpointReq) -> Self {
        Self {
            url: req.url.clone(),
            tenant: "".to_string(),
            args: req.args.clone(),
            credential: req.credential.clone(),
            comment: req.comment.clone(),
            create_on: req.create_on,
        }
    }

    pub fn if_need_to_upsert(&self, req: &UpsertShareEndpointReq) -> bool {
        if self.url != req.url || self.args != req.args || self.credential != req.credential {
            return true;
        };

        true
    }

    pub fn upsert(&self, req: &UpsertShareEndpointReq) -> Self {
        let mut meta = self.clone();

        meta.url = req.url.clone();
        meta.args = req.args.clone();
        meta.credential = req.credential.clone();
        meta.create_on = req.create_on;

        meta
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareAccountMeta {
    pub account: String,
    pub share_id: u64,
    pub share_on: DateTime<Utc>,
    pub accept_on: Option<DateTime<Utc>>,
}

impl ShareAccountMeta {
    pub fn new(account: String, share_id: u64, share_on: DateTime<Utc>) -> Self {
        Self {
            account,
            share_id,
            share_on,
            accept_on: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareId {
    pub share_id: u64,
}

impl ShareId {
    pub fn new(share_id: u64) -> Self {
        Self { share_id }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareIdToName {
    pub share_id: u64,
}

impl Display for ShareIdToName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.share_id)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareEndpointId {
    pub share_endpoint_id: u64,
}

impl ShareEndpointId {
    pub fn new(share_endpoint_id: u64) -> Self {
        Self { share_endpoint_id }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareEndpointIdToName {
    pub share_endpoint_id: u64,
}

impl Display for ShareEndpointIdToName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.share_endpoint_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObject {
    Database(u64),
    Table(u64),
}

impl ShareGrantObject {
    pub fn new(seq_and_id: &ShareGrantObjectSeqAndId) -> ShareGrantObject {
        match seq_and_id {
            ShareGrantObjectSeqAndId::Database(_seq, db_id, _meta) => {
                ShareGrantObject::Database(*db_id)
            }
            ShareGrantObjectSeqAndId::Table(_db_id, _seq, table_id, _meta) => {
                ShareGrantObject::Table(*table_id)
            }
        }
    }
}

impl Display for ShareGrantObject {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareGrantObject::Database(db_id) => {
                write!(f, "db/{}", *db_id)
            }
            ShareGrantObject::Table(table_id) => {
                write!(f, "table/{}", *table_id)
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectSharedByShareIds {
    // save share ids which shares this object
    pub share_ids: BTreeSet<u64>,
}

impl Default for ObjectSharedByShareIds {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectSharedByShareIds {
    pub fn new() -> ObjectSharedByShareIds {
        ObjectSharedByShareIds {
            share_ids: BTreeSet::new(),
        }
    }

    pub fn add(&mut self, share_id: u64) {
        self.share_ids.insert(share_id);
    }

    pub fn remove(&mut self, share_id: u64) {
        self.share_ids.remove(&share_id);
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

impl From<databend_common_ast::ast::ShareGrantObjectPrivilege> for ShareGrantObjectPrivilege {
    fn from(privilege: databend_common_ast::ast::ShareGrantObjectPrivilege) -> Self {
        match privilege {
            databend_common_ast::ast::ShareGrantObjectPrivilege::Usage => {
                ShareGrantObjectPrivilege::Usage
            }
            databend_common_ast::ast::ShareGrantObjectPrivilege::ReferenceUsage => {
                ShareGrantObjectPrivilege::ReferenceUsage
            }
            databend_common_ast::ast::ShareGrantObjectPrivilege::Select => {
                ShareGrantObjectPrivilege::Select
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantEntry {
    pub object: ShareGrantObject,
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub grant_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

impl ShareGrantEntry {
    pub fn new(
        object: ShareGrantObject,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) -> Self {
        Self {
            object,
            privileges: BitFlags::from(privileges),
            grant_on,
            update_on: None,
        }
    }

    pub fn grant_privileges(
        &mut self,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) {
        self.update_on = Some(grant_on);
        self.privileges = BitFlags::from(privileges);
    }

    // return true if all privileges are empty.
    pub fn revoke_privileges(
        &mut self,
        privileges: ShareGrantObjectPrivilege,
        update_on: DateTime<Utc>,
    ) -> bool {
        self.update_on = Some(update_on);
        self.privileges.remove(BitFlags::from(privileges));
        self.privileges.is_empty()
    }

    pub fn object(&self) -> &ShareGrantObject {
        &self.object
    }

    pub fn privileges(&self) -> &BitFlags<ShareGrantObjectPrivilege> {
        &self.privileges
    }

    pub fn has_granted_privileges(&self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.contains(privileges)
    }
}

impl Display for ShareGrantEntry {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.object)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ShareMeta {
    pub database: Option<ShareGrantEntry>,
    pub entries: BTreeMap<String, ShareGrantEntry>,

    // save accounts which has been granted access to this share.
    pub accounts: BTreeSet<String>,
    pub comment: Option<String>,
    pub share_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,

    // save db ids which created from this share
    pub share_from_db_ids: BTreeSet<u64>,
}

impl ShareMeta {
    pub fn new(share_on: DateTime<Utc>, comment: Option<String>) -> Self {
        ShareMeta {
            share_on,
            comment,
            ..Default::default()
        }
    }

    pub fn get_accounts(&self) -> Vec<String> {
        Vec::<String>::from_iter(self.accounts.clone())
    }

    pub fn has_account(&self, account: &String) -> bool {
        self.accounts.contains(account)
    }

    pub fn add_account(&mut self, account: String) {
        self.accounts.insert(account);
    }

    pub fn del_account(&mut self, account: &str) {
        self.accounts.remove(account);
    }

    pub fn has_share_from_db_id(&self, db_id: u64) -> bool {
        self.share_from_db_ids.contains(&db_id)
    }

    pub fn add_share_from_db_id(&mut self, db_id: u64) {
        self.share_from_db_ids.insert(db_id);
    }

    pub fn remove_share_from_db_id(&mut self, db_id: u64) {
        self.share_from_db_ids.remove(&db_id);
    }

    pub fn get_grant_entry(&self, object: ShareGrantObject) -> Option<ShareGrantEntry> {
        let database = self.database.as_ref()?;
        if database.object == object {
            return Some(database.clone());
        }

        match object {
            ShareGrantObject::Database(_db_id) => None,
            ShareGrantObject::Table(_table_id) => self.entries.get(&object.to_string()).cloned(),
        }
    }

    pub fn grant_object_privileges(
        &mut self,
        object: ShareGrantObject,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) {
        let key = object.to_string();

        match object {
            ShareGrantObject::Database(_db_id) => {
                if let Some(db) = &mut self.database {
                    db.grant_privileges(privileges, grant_on);
                } else {
                    self.database = Some(ShareGrantEntry::new(object, privileges, grant_on));
                }
            }
            ShareGrantObject::Table(_table_id) => {
                match self.entries.get_mut(&key) {
                    Some(entry) => {
                        entry.grant_privileges(privileges, grant_on);
                    }
                    None => {
                        let entry = ShareGrantEntry::new(object, privileges, grant_on);
                        self.entries.insert(key, entry);
                    }
                };
            }
        }
    }

    pub fn has_granted_privileges(
        &self,
        obj_name: &ShareGrantObjectName,
        object: &ShareGrantObjectSeqAndId,
        privileges: ShareGrantObjectPrivilege,
    ) -> Result<bool, AppError> {
        match object {
            ShareGrantObjectSeqAndId::Database(_seq, db_id, _meta) => match &self.database {
                Some(db) => match db.object {
                    ShareGrantObject::Database(self_db_id) => {
                        if self_db_id != *db_id {
                            Err(AppError::WrongShareObject(WrongShareObject::new(
                                obj_name.to_string(),
                            )))
                        } else {
                            Ok(db.has_granted_privileges(privileges))
                        }
                    }
                    ShareGrantObject::Table(_) => {
                        unreachable!("grant database CANNOT be a table");
                    }
                },
                None => Ok(false),
            },
            ShareGrantObjectSeqAndId::Table(_db_id, _table_seq, table_id, _meta) => {
                let key = ShareGrantObject::Table(*table_id).to_string();
                Ok(self
                    .entries
                    .get(&key)
                    .map_or(false, |entry| entry.has_granted_privileges(privileges)))
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareIdent {
    pub share_id: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareInfo {
    pub ident: ShareIdent,
    pub name_ident: ShareNameIdentRaw,
    pub meta: ShareMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareTableSpec {
    pub name: String,
    pub database_id: u64,
    pub table_id: u64,
    pub presigned_url_timeout: String,
}

impl ShareTableSpec {
    pub fn new(name: &str, database_id: u64, table_id: u64) -> Self {
        ShareTableSpec {
            name: name.to_owned(),
            database_id,
            table_id,
            presigned_url_timeout: "120s".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareDatabaseSpec {
    pub name: String,
    pub id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpec {
    pub name: String,
    pub share_id: u64,
    pub version: u64,
    pub database: Option<ShareDatabaseSpec>,
    pub tables: Vec<ShareTableSpec>,
    pub tenants: Vec<String>,
    pub db_privileges: Option<BitFlags<ShareGrantObjectPrivilege>>,
    pub comment: Option<String>,
    pub share_on: Option<DateTime<Utc>>,
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use super::ShareEndpointId;
    use crate::schema::DatabaseId;
    use crate::schema::TableId;
    use crate::share::share_end_point_ident::ShareEndpointIdentRaw;
    use crate::share::share_name_ident::ShareNameIdentRaw;
    use crate::share::ObjectSharedByShareIds;
    use crate::share::ShareEndpointIdToName;
    use crate::share::ShareEndpointMeta;
    use crate::share::ShareGrantObject;
    use crate::share::ShareId;
    use crate::share::ShareIdToName;
    use crate::share::ShareMeta;

    impl kvapi::KeyCodec for ShareGrantObject {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            match self {
                ShareGrantObject::Database(db_id) => b.push_raw("db").push_u64(*db_id),
                ShareGrantObject::Table(table_id) => b.push_raw("table").push_u64(*table_id),
            }
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let kind = parser.next_raw()?;
            let id = parser.next_u64()?;

            match kind {
                "db" => Ok(ShareGrantObject::Database(id)),
                "table" => Ok(ShareGrantObject::Table(id)),
                _ => Err(kvapi::KeyError::InvalidSegment {
                    i: 1,
                    expect: "db or table".to_string(),
                    got: kind.to_string(),
                }),
            }
        }
    }

    /// __fd_share_by/{db|table}/<object_id> -> ObjectSharedByShareIds
    impl kvapi::Key for ShareGrantObject {
        const PREFIX: &'static str = "__fd_share_by";

        type ValueType = ObjectSharedByShareIds;

        fn parent(&self) -> Option<String> {
            let k = match self {
                ShareGrantObject::Database(db_id) => DatabaseId::new(*db_id).to_string_key(),
                ShareGrantObject::Table(table_id) => TableId::new(*table_id).to_string_key(),
            };
            Some(k)
        }
    }

    impl kvapi::KeyCodec for ShareId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.share_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let share_id = parser.next_u64()?;

            Ok(Self { share_id })
        }
    }

    /// __fd_share_id/<share_id> -> <share_meta>
    impl kvapi::Key for ShareId {
        const PREFIX: &'static str = "__fd_share_id";

        type ValueType = ShareMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for ShareIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.share_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let share_id = parser.next_u64()?;

            Ok(Self { share_id })
        }
    }

    /// __fd_share_id_to_name/<share_id> -> ShareNameIdent
    impl kvapi::Key for ShareIdToName {
        const PREFIX: &'static str = "__fd_share_id_to_name";

        type ValueType = ShareNameIdentRaw;

        fn parent(&self) -> Option<String> {
            Some(ShareId::new(self.share_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for ShareEndpointId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.share_endpoint_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let share_endpoint_id = parser.next_u64()?;

            Ok(Self { share_endpoint_id })
        }
    }

    /// __fd_share_endpoint_id/<share_endpoint_id> -> <share_meta>
    impl kvapi::Key for ShareEndpointId {
        const PREFIX: &'static str = "__fd_share_endpoint_id";

        type ValueType = ShareEndpointMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for ShareEndpointIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.share_endpoint_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let share_endpoint_id = parser.next_u64()?;

            Ok(Self { share_endpoint_id })
        }
    }

    /// __fd_share_endpoint_id_to_name/<share_endpoint_id> -> ShareEndpointIdent
    impl kvapi::Key for ShareEndpointIdToName {
        const PREFIX: &'static str = "__fd_share_endpoint_id_to_name";

        /// ShareEndpointIdent must contain a initialized Tenant,
        /// The value loaded from meta-service is not a initialized one.
        type ValueType = ShareEndpointIdentRaw;

        fn parent(&self) -> Option<String> {
            Some(ShareEndpointId::new(self.share_endpoint_id).to_string_key())
        }
    }

    impl kvapi::Value for ObjectSharedByShareIds {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for ShareMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for ShareNameIdentRaw {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for ShareEndpointMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for ShareEndpointIdentRaw {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

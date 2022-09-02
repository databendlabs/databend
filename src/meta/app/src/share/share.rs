// Copyright 2022 Datafuse Labs.
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

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_types::app_error::AppError;
use common_meta_types::app_error::WrongShareObject;
use common_meta_types::MetaError;
use enumflags2::bitflags;
use enumflags2::BitFlags;

use crate::schema::DatabaseMeta;
use crate::schema::TableMeta;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareNameIdent {
    pub tenant: String,
    pub share_name: String,
}

impl Display for ShareNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.share_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareAccountNameIdent {
    pub account: String,
    pub share_id: u64,
}

impl Display for ShareAccountNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.account, self.share_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesReq {
    pub tenant: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShowSharesReply {
    // sharing to other accounts(outbound shares)
    pub outbound_accounts: Vec<ShareAccountReply>,
    // be shared by other accounts(inbound shares)
    pub inbound_accounts: Vec<ShareAccountReply>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReq {
    pub if_not_exists: bool,
    pub share_name: ShareNameIdent,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateShareReply {
    pub share_id: u64,

    pub spec: Option<ShareSpec>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropShareReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropShareReply {
    pub share_id: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AddShareAccountsReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
    pub accounts: Vec<String>,
    pub share_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AddShareAccountsReply {
    pub share_id: Option<u64>,
    pub spec: Option<ShareSpec>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RemoveShareAccountsReq {
    pub share_name: ShareNameIdent,
    pub if_exists: bool,
    pub accounts: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RemoveShareAccountsReply {
    pub share_id: Option<u64>,
    pub spec: Option<ShareSpec>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareGrantObjectSeqAndId {
    // db_meta_seq, db_id, DatabaseMeta
    Database(u64, u64, DatabaseMeta),
    // db_id, table_meta_seq, table_id, table_meta
    Table(u64, u64, u64, TableMeta),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GrantShareObjectReq {
    pub share_name: ShareNameIdent,
    pub object: ShareGrantObjectName,
    pub grant_on: DateTime<Utc>,
    pub privilege: ShareGrantObjectPrivilege,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GrantShareObjectReply {
    pub share_id: u64,
    pub spec: ShareSpec,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RevokeShareObjectReq {
    pub share_name: ShareNameIdent,
    pub object: ShareGrantObjectName,
    pub privilege: ShareGrantObjectPrivilege,
    pub update_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RevokeShareObjectReply {
    pub share_id: u64,
    pub spec: ShareSpec,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantObjectReq {
    pub share_name: ShareNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantReplyObject {
    pub object: ShareGrantObjectName,
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub grant_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantObjectReply {
    pub share_name: ShareNameIdent,
    pub objects: Vec<ShareGrantReplyObject>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenantsReq {
    pub share_name: ShareNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenants {
    pub account: String,
    pub grant_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetShareGrantTenantsReply {
    pub accounts: Vec<GetShareGrantTenants>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetObjectGrantPrivilegesReq {
    pub tenant: String,
    pub object: ShareGrantObjectName,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectGrantPrivilege {
    pub share_name: String,
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub grant_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetObjectGrantPrivilegesReply {
    pub privileges: Vec<ObjectGrantPrivilege>,
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareId {
    pub share_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareIdToName {
    pub share_id: u64,
}

impl Display for ShareIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.share_id)
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl Display for ShareGrantObjectPrivilege {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            ShareGrantObjectPrivilege::Usage => write!(f, "USAGE"),
            ShareGrantObjectPrivilege::ReferenceUsage => write!(f, "REFERENCE_USAGE"),
            ShareGrantObjectPrivilege::Select => write!(f, "SELECT"),
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        Vec::<String>::from_iter(self.accounts.clone().into_iter())
    }

    pub fn has_account(&self, account: &String) -> bool {
        self.accounts.contains(account)
    }

    pub fn add_account(&mut self, account: String) {
        self.accounts.insert(account);
    }

    pub fn del_account(&mut self, account: &String) {
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

    pub fn revoke_object_privileges(
        &mut self,
        object: ShareGrantObject,
        privileges: ShareGrantObjectPrivilege,
        update_on: DateTime<Utc>,
    ) -> Result<(), MetaError> {
        let key = object.to_string();

        match object {
            ShareGrantObject::Database(_db_id) => {
                if let Some(entry) = &mut self.database {
                    if object == entry.object {
                        if entry.revoke_privileges(privileges, update_on) {
                            // all database privileges have been revoked, clear database and entries.
                            self.database = None;
                            self.entries.clear();
                            self.update_on = Some(update_on);
                        }
                    } else {
                        return Err(MetaError::AppError(AppError::WrongShareObject(
                            WrongShareObject::new(&key),
                        )));
                    }
                } else {
                    return Err(MetaError::AppError(AppError::WrongShareObject(
                        WrongShareObject::new(object.to_string()),
                    )));
                }
            }
            ShareGrantObject::Table(table_id) => match self.entries.get_mut(&key) {
                Some(entry) => {
                    if let ShareGrantObject::Table(self_table_id) = entry.object {
                        if self_table_id == table_id {
                            if entry.revoke_privileges(privileges, update_on) {
                                self.entries.remove(&key);
                            }
                        } else {
                            return Err(MetaError::AppError(AppError::WrongShareObject(
                                WrongShareObject::new(object.to_string()),
                            )));
                        }
                    } else {
                        unreachable!("ShareMeta.entries MUST be Table Object");
                    }
                }
                None => return Ok(()),
            },
        }
        Ok(())
    }

    pub fn has_granted_privileges(
        &self,
        obj_name: &ShareGrantObjectName,
        object: &ShareGrantObjectSeqAndId,
        privileges: ShareGrantObjectPrivilege,
    ) -> Result<bool, MetaError> {
        match object {
            ShareGrantObjectSeqAndId::Database(_seq, db_id, _meta) => match &self.database {
                Some(db) => match db.object {
                    ShareGrantObject::Database(self_db_id) => {
                        if self_db_id != *db_id {
                            Err(MetaError::AppError(AppError::WrongShareObject(
                                WrongShareObject::new(obj_name.to_string()),
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
                match self.entries.get(&key) {
                    Some(entry) => Ok(entry.has_granted_privileges(privileges)),
                    None => Ok(false),
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareIdent {
    pub share_id: u64,
    pub seq: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareInfo {
    pub ident: ShareIdent,
    pub name_ident: ShareNameIdent,
    pub meta: ShareMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareTableSpec {
    pub name: String,
    pub database_id: u64,
    pub table_id: u64,
    pub version: u64,
    pub presigned_url_timeout: String,
}

impl ShareTableSpec {
    pub fn new(name: &str, database_id: u64, table_id: u64) -> Self {
        ShareTableSpec {
            name: name.to_owned(),
            database_id,
            table_id,
            version: 1,
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
    pub database: Option<ShareDatabaseSpec>,
    pub tables: Vec<ShareTableSpec>,
    pub tenants: Vec<String>,
}

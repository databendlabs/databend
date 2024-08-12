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
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use enumflags2::BitFlags;

use crate::app_error::AppError;
use crate::app_error::WrongShareObject;
use crate::share::ShareGrantObject;
use crate::share::ShareGrantObjectName;
use crate::share::ShareGrantObjectPrivilege;
use crate::share::ShareGrantObjectSeqAndId;

// a unify data struct for saving granted share object: database, table
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
pub struct ShareMetaV1 {
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

impl ShareMetaV1 {
    pub fn new(share_on: DateTime<Utc>, comment: Option<String>) -> Self {
        ShareMetaV1 {
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
            _ => None,
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
            _ => {}
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
                    ShareGrantObject::Table(_) | ShareGrantObject::View(_) => {
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
            ShareGrantObjectSeqAndId::View(..) => {
                unreachable!()
            }
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::share::ShareMetaV1;

    impl kvapi::Value for ShareMetaV1 {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

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

use std::collections::BTreeSet;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use enumflags2::BitFlags;

use crate::app_error::AppError;
use crate::app_error::WrongShareObject;
use crate::share::ShareGrantObjectPrivilege;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareDatabase {
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub name: String,
    pub db_id: u64,
    pub grant_on: DateTime<Utc>,
}

impl ShareDatabase {
    pub fn grant_privileges(
        &mut self,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) {
        self.grant_on = grant_on;
        self.privileges.insert(BitFlags::from(privileges));
    }

    pub fn revoke_object_privileges(&mut self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.remove(BitFlags::from(privileges));
        self.privileges.is_empty()
    }

    pub fn has_granted_privileges(&self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.contains(privileges)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareTable {
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub name: String,
    pub db_id: u64,
    pub table_id: u64,
    pub grant_on: DateTime<Utc>,
    // fuse/view
    pub engine: String,
    // if table is a view, save all the reference table ids
    pub reference_table: BTreeSet<u64>,
}

impl ShareTable {
    pub fn grant_privileges(
        &mut self,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) {
        self.grant_on = grant_on;
        self.privileges.insert(BitFlags::from(privileges));
    }

    pub fn revoke_object_privileges(&mut self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.remove(BitFlags::from(privileges));
        self.privileges.is_empty()
    }

    pub fn has_granted_privileges(&self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.contains(privileges)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShareReferenceTable {
    pub privileges: BitFlags<ShareGrantObjectPrivilege>,
    pub name: String,
    pub db_id: u64,
    pub table_id: u64,
    pub grant_on: DateTime<Utc>,
    // fuse/view
    pub engine: String,
    // reference by view ids
    pub reference_by: BTreeSet<u64>,
}

impl ShareReferenceTable {
    pub fn grant_privileges(
        &mut self,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) {
        self.grant_on = grant_on;
        self.privileges.insert(BitFlags::from(privileges));
    }

    pub fn revoke_object_privileges(&mut self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.remove(BitFlags::from(privileges));
        self.privileges.is_empty()
    }

    pub fn has_granted_privileges(&self, privileges: ShareGrantObjectPrivilege) -> bool {
        self.privileges.contains(privileges)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ShareMetaV2 {
    // save accounts which has been granted access to this share.
    pub accounts: BTreeSet<String>,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
    pub update_on: DateTime<Utc>,

    // share objects
    // only one database can be grant used in one share
    pub use_database: Option<ShareDatabase>,
    pub reference_database: Vec<ShareDatabase>,
    pub table: Vec<ShareTable>,
    pub reference_table: Vec<ShareReferenceTable>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShareObject {
    // (db name, db id)
    Database(String, u64),
    // (table name, db_id, table id)
    Table(String, u64, u64),
}

impl Display for ShareObject {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShareObject::Database(name, _db_id) => {
                write!(f, "database {}", name)
            }
            ShareObject::Table(name, _db_id, _table_id) => {
                write!(f, "table {}", name)
            }
        }
    }
}

impl ShareMetaV2 {
    pub fn new(create_on: DateTime<Utc>, comment: Option<String>) -> Self {
        Self {
            create_on,
            update_on: create_on,
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

    // get_grant_entry
    pub fn get_share_table(&self, table_id: u64) -> Option<ShareTable> {
        for table in &self.table {
            if table.table_id == table_id {
                return Some(table.clone());
            }
        }

        None
    }

    pub fn grant_object_privileges(
        &mut self,
        object: &ShareObject,
        privileges: ShareGrantObjectPrivilege,
        grant_on: DateTime<Utc>,
    ) -> Result<(), AppError> {
        match object {
            ShareObject::Database(name, db_id) => match privileges {
                ShareGrantObjectPrivilege::Usage => {
                    if let Some(use_database) = &mut self.use_database {
                        if use_database.db_id != *db_id {
                            return Err(AppError::WrongShareObject(WrongShareObject::new(
                                object.to_string(),
                            )));
                        }
                    } else {
                        self.use_database = Some(ShareDatabase {
                            privileges: BitFlags::from(privileges),
                            name: name.to_string(),
                            grant_on,
                            db_id: *db_id,
                        });
                    }
                }
                ShareGrantObjectPrivilege::ReferenceUsage => {
                    for db in &mut self.reference_database {
                        if db.db_id == *db_id {
                            return Ok(());
                        }
                    }
                    self.reference_database.push(ShareDatabase {
                        privileges: BitFlags::from(privileges),
                        name: name.to_string(),
                        grant_on,
                        db_id: *db_id,
                    });
                }
                _ => {}
            },

            ShareObject::Table(name, db_id, table_id) => match privileges {
                ShareGrantObjectPrivilege::Select => {
                    for table in &mut self.table {
                        if table.table_id == *table_id {
                            return Ok(());
                        }
                    }
                    self.table.push(ShareTable {
                        privileges: BitFlags::from(privileges),
                        name: name.to_string(),
                        db_id: *db_id,
                        grant_on,
                        table_id: *table_id,
                        engine: "FUSE".to_string(),
                        reference_table: BTreeSet::new(),
                    })
                }
                ShareGrantObjectPrivilege::ReferenceUsage => {
                    for table in &mut self.reference_table {
                        if table.table_id == *table_id {
                            return Ok(());
                        }
                    }

                    self.reference_table.push(ShareReferenceTable {
                        privileges: BitFlags::from(privileges),
                        name: name.to_string(),
                        db_id: *db_id,
                        grant_on,
                        table_id: *table_id,
                        engine: "FUSE".to_string(),
                        reference_by: BTreeSet::new(),
                    })
                }
                _ => {}
            },
        }

        Ok(())
    }

    pub fn has_granted_privileges(
        &self,
        object: &ShareObject,
        privileges: ShareGrantObjectPrivilege,
    ) -> Result<bool, AppError> {
        match object {
            ShareObject::Database(_name, db_id) => {
                if let Some(use_database) = &self.use_database {
                    if use_database.db_id == *db_id {
                        return Ok(use_database.has_granted_privileges(privileges));
                    }
                }

                for db in &self.reference_database {
                    if db.db_id == *db_id {
                        return Ok(db.has_granted_privileges(privileges));
                    }
                }
            }
            ShareObject::Table(_table_name, _db_id, table_id) => {
                for table in &self.table {
                    if table.table_id == *table_id {
                        return Ok(table.has_granted_privileges(privileges));
                    }
                }
            }
        }

        Ok(false)
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::share::ShareMetaV2;

    impl kvapi::Value for ShareMetaV2 {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

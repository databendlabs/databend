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

use enumflags2::BitFlags;

use crate::UserPrivilege;
use crate::UserPrivilegeType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum GrantObject {
    Global,
    Database(String),
    Table(String, String),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GrantEntry {
    user: String,
    host_pattern: String,
    object: GrantObject,
    privileges: BitFlags<UserPrivilegeType>,
}

impl GrantEntry {
    pub fn new(
        user: String,
        host_pattern: String,
        object: GrantObject,
        privileges: BitFlags<UserPrivilegeType>,
    ) -> Self {
        Self {
            user,
            host_pattern,
            object,
            privileges,
        }
    }

    pub fn verify_global_privilege(
        &self,
        user: &str,
        host: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        if !self.matches_user_host(user, host) {
            return false;
        }

        if self.object != GrantObject::Global {
            return false;
        }

        self.privileges.contains(privilege)
    }

    pub fn verify_database_privilege(
        &self,
        user: &str,
        host: &str,
        db: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        if !self.matches_user_host(user, host) {
            return false;
        }

        if !match &self.object {
            GrantObject::Global => true,
            GrantObject::Database(ref expected_db) => expected_db == db,
            _ => false,
        } {
            return false;
        }

        self.privileges.contains(privilege)
    }

    pub fn verify_table_privilege(
        &self,
        user: &str,
        host: &str,
        db: &str,
        table: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        if !self.matches_user_host(user, host) {
            return false;
        }

        if !match &self.object {
            GrantObject::Global => true,
            GrantObject::Database(ref expected_db) => expected_db == db,
            GrantObject::Table(ref expected_db, ref expected_table) => {
                expected_db == db && expected_table == table
            }
        } {
            return false;
        }

        self.privileges.contains(privilege)
    }

    pub fn matches_entry(&self, user: &str, host_pattern: &str, object: &GrantObject) -> bool {
        self.user == user && self.host_pattern == host_pattern && &self.object == object
    }

    fn matches_user_host(&self, user: &str, host: &str) -> bool {
        self.user == user && Self::match_host_pattern(&self.host_pattern, host)
    }

    fn match_host_pattern(host_pattern: &str, host: &str) -> bool {
        // TODO: support IP pattern like 0.2.%.%
        if host_pattern == "%" {
            return true;
        }
        host_pattern == host
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct UserGrantSet {
    grants: Vec<GrantEntry>,
}

impl UserGrantSet {
    pub fn empty() -> Self {
        Self { grants: vec![] }
    }

    pub fn entries(&self) -> &[GrantEntry] {
        &self.grants
    }

    pub fn verify_global_privilege(
        &self,
        user: &str,
        host: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        self.grants
            .iter()
            .any(|e| e.verify_global_privilege(user, host, privilege))
    }

    pub fn verify_database_privilege(
        &self,
        user: &str,
        host: &str,
        db: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        self.grants
            .iter()
            .any(|e| e.verify_database_privilege(user, host, db, privilege))
    }

    pub fn verify_table_privilege(
        &self,
        user: &str,
        host: &str,
        db: &str,
        table: &str,
        privilege: UserPrivilegeType,
    ) -> bool {
        self.grants
            .iter()
            .any(|e| e.verify_table_privilege(user, host, db, table, privilege))
    }

    pub fn grant_privileges(
        &mut self,
        user: &str,
        host_pattern: &str,
        object: &GrantObject,
        privileges: UserPrivilege,
    ) {
        let privileges: BitFlags<UserPrivilegeType> = privileges.into();
        let mut new_grants: Vec<GrantEntry> = vec![];
        let mut changed = false;

        for grant in self.grants.iter() {
            let mut grant = grant.clone();
            if grant.matches_entry(user, host_pattern, object) {
                grant.privileges |= privileges;
                changed = true;
            }
            new_grants.push(grant);
        }

        if !changed {
            new_grants.push(GrantEntry::new(
                user.into(),
                host_pattern.into(),
                object.clone(),
                privileges,
            ))
        }

        self.grants = new_grants;
    }

    pub fn revoke_privileges(
        &mut self,
        user: &str,
        host_pattern: &str,
        object: &GrantObject,
        privileges: UserPrivilege,
    ) {
        let privileges: BitFlags<UserPrivilegeType> = privileges.into();
        let grants = self
            .grants
            .iter()
            .map(|e| {
                if e.matches_entry(user, host_pattern, object) {
                    let mut e = e.clone();
                    e.privileges ^= privileges;
                    e
                } else {
                    e.clone()
                }
            })
            .filter(|e| e.privileges != BitFlags::empty())
            .collect::<Vec<_>>();
        self.grants = grants;
    }
}

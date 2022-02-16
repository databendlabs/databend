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

use std::collections::HashSet;
use std::fmt;

use enumflags2::BitFlags;

use crate::role_identity::RoleIdentity;
use crate::UserPrivilegeSet;
use crate::UserPrivilegeType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum GrantObject {
    Global,
    Database(String),
    Table(String, String),
}

impl GrantObject {
    /// Comparing the grant objects, the Database object contains all the Table objects inside it.
    /// Global object contains all the Database objects.
    pub fn contains(&self, object: &GrantObject) -> bool {
        match (self, object) {
            (GrantObject::Global, _) => true,
            (GrantObject::Database(_), GrantObject::Global) => false,
            (GrantObject::Database(lhs), GrantObject::Database(rhs)) => lhs == rhs,
            (GrantObject::Database(lhs), GrantObject::Table(rhs, _)) => lhs == rhs,
            (GrantObject::Table(lhs_db, lhs_table), GrantObject::Table(rhs_db, rhs_table)) => {
                (lhs_db == rhs_db) && (lhs_table == rhs_table)
            }
            (GrantObject::Table(_, _), _) => false,
        }
    }

    /// Global, database and table has different available privileges
    pub fn available_privileges(&self) -> UserPrivilegeSet {
        match self {
            GrantObject::Global => UserPrivilegeSet::available_privileges_on_global(),
            GrantObject::Database(_) => UserPrivilegeSet::available_privileges_on_database(),
            GrantObject::Table(_, _) => UserPrivilegeSet::available_privileges_on_table(),
        }
    }
}

impl fmt::Display for GrantObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            GrantObject::Global => write!(f, "*.*"),
            GrantObject::Database(ref db) => write!(f, "'{}'.*", db),
            GrantObject::Table(ref db, ref table) => write!(f, "'{}'.'{}'", db, table),
        }
    }
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

    pub fn verify_privilege(&self, object: &GrantObject, privilege: UserPrivilegeType) -> bool {
        // the verified object should be smaller than the object inside my grant entry.
        if !self.object.contains(object) {
            return false;
        }

        self.privileges.contains(privilege)
    }

    pub fn matches_entry(&self, object: &GrantObject) -> bool {
        &self.object == object
    }

    fn has_all_available_privileges(&self) -> bool {
        let all_available_privileges = self.object.available_privileges();
        self.privileges
            .contains(BitFlags::from(all_available_privileges))
    }
}

// TODO: remove user and host fields in the GrantEntry struct
impl fmt::Display for GrantEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        let privileges: UserPrivilegeSet = self.privileges.into();
        let privileges_str = if self.has_all_available_privileges() {
            "ALL".to_string()
        } else {
            privileges.to_string()
        };
        write!(
            f,
            "GRANT {} ON {} TO '{}'@'{}'",
            &privileges_str, self.object, self.user, self.host_pattern
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct UserGrantSet {
    entries: Vec<GrantEntry>,
    roles: HashSet<RoleIdentity>,
}

impl UserGrantSet {
    pub fn empty() -> Self {
        Self {
            entries: vec![],
            roles: HashSet::new(),
        }
    }

    pub fn entries(&self) -> &[GrantEntry] {
        &self.entries
    }

    pub fn roles(&self) -> Vec<RoleIdentity> {
        self.roles.iter().cloned().collect::<Vec<_>>()
    }

    pub fn grant_role(&mut self, role: RoleIdentity) {
        self.roles.insert(role);
    }

    pub fn revoke_role(&mut self, role: &RoleIdentity) {
        self.roles.remove(role);
    }

    pub fn verify_privilege(&self, object: &GrantObject, privilege: UserPrivilegeType) -> bool {
        self.entries
            .iter()
            .any(|e| e.verify_privilege(object, privilege))
    }

    // TODO: remove the user and host parameters
    pub fn grant_privileges(
        &mut self,
        user: &str,
        host_pattern: &str,
        object: &GrantObject,
        privileges: UserPrivilegeSet,
    ) {
        let privileges: BitFlags<UserPrivilegeType> = privileges.into();
        let mut new_entries: Vec<GrantEntry> = vec![];
        let mut changed = false;

        for entry in self.entries.iter() {
            let mut entry = entry.clone();
            if entry.matches_entry(object) {
                entry.privileges |= privileges;
                changed = true;
            }
            new_entries.push(entry);
        }

        if !changed {
            new_entries.push(GrantEntry::new(
                user.into(),
                host_pattern.into(),
                object.clone(),
                privileges,
            ))
        }

        self.entries = new_entries;
    }

    pub fn revoke_privileges(&mut self, object: &GrantObject, privileges: UserPrivilegeSet) {
        let privileges: BitFlags<UserPrivilegeType> = privileges.into();
        let new_entries = self
            .entries
            .iter()
            .map(|e| {
                if e.matches_entry(object) {
                    let mut e = e.clone();
                    e.privileges ^= privileges;
                    e
                } else {
                    e.clone()
                }
            })
            .filter(|e| e.privileges != BitFlags::empty())
            .collect::<Vec<_>>();
        self.entries = new_entries;
    }
}

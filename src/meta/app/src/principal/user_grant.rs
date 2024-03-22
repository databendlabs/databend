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

use std::collections::HashSet;
use std::fmt;
use std::ops;

use databend_common_meta_kvapi::kvapi;
use enumflags2::BitFlags;

use crate::principal::UserPrivilegeSet;
use crate::principal::UserPrivilegeType;
use crate::tenant::Tenant;

/// [`OwnershipObject`] is used to maintain the grant object that support rename by id. Using ID over name
/// have many benefits, it can avoid lost privileges after the object get renamed.
/// But Stage and UDF do not support the concept of renaming and do not have ids, so names can be used.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum OwnershipObject {
    /// used on the fuse databases
    Database {
        catalog_name: String,
        db_id: u64,
    },

    /// used on the fuse tables
    Table {
        catalog_name: String,
        db_id: u64,
        table_id: u64,
    },

    Stage {
        name: String,
    },

    UDF {
        name: String,
    },
}

impl OwnershipObject {
    // # Legacy issue: Catalog is not encoded into key.
    //
    // But at that time, only `"default"` catalog is used.
    // Thus we follow the same rule, if catalog is `"default"`, we don't encode it.
    //
    // This issue is introduced in https://github.com/drmingdrmer/databend/blob/7681763dc54306e55b5e0326af0510292d244be3/src/query/management/src/role/role_mgr.rs#L86
    const DEFAULT_CATALOG: &'static str = "default";

    /// Build key with the provided KeyBuilder as sub part of key used to access meta-service
    pub(crate) fn build_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        match self {
            OwnershipObject::Database {
                catalog_name,
                db_id,
            } => {
                // Legacy issue: Catalog is not encoded into key.
                if catalog_name == Self::DEFAULT_CATALOG {
                    b.push_raw("database-by-id").push_u64(*db_id)
                } else {
                    b.push_raw("database-by-catalog-id")
                        .push_str(catalog_name)
                        .push_u64(*db_id)
                }
            }
            OwnershipObject::Table {
                catalog_name,
                db_id,
                table_id,
            } => {
                // TODO(flaneur): db_id is not encoded into key. Thus such key can not be decoded.
                let _ = db_id;

                // Legacy issue: Catalog is not encoded into key.
                if catalog_name == Self::DEFAULT_CATALOG {
                    b.push_raw("table-by-id").push_u64(*table_id)
                } else {
                    b.push_raw("table-by-catalog-id")
                        .push_str(catalog_name)
                        .push_u64(*table_id)
                }
            }
            OwnershipObject::Stage { name } => b.push_raw("stage-by-name").push_str(name),
            OwnershipObject::UDF { name } => b.push_raw("udf-by-name").push_str(name),
        }
    }

    /// Parse encoded key and return the OwnershipObject
    pub(crate) fn parse_key(p: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
        let q = p.next_raw()?;
        match q {
            "database-by-id" => {
                let db_id = p.next_u64()?;
                Ok(OwnershipObject::Database {
                    catalog_name: Self::DEFAULT_CATALOG.to_string(),
                    db_id,
                })
            }
            "database-by-catalog-id" => {
                let catalog_name = p.next_str()?;
                let db_id = p.next_u64()?;
                Ok(OwnershipObject::Database {
                    catalog_name,
                    db_id,
                })
            }
            "table-by-id" => {
                let table_id = p.next_u64()?;
                Ok(OwnershipObject::Table {
                    catalog_name: Self::DEFAULT_CATALOG.to_string(),
                    db_id: 0,
                    table_id,
                })
            }
            "table-by-catalog-id" => {
                let catalog_name = p.next_str()?;
                let table_id = p.next_u64()?;
                Ok(OwnershipObject::Table {
                    catalog_name,
                    db_id: 0, // string key does not contain db_id
                    table_id,
                })
            }
            "stage-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::Stage { name })
            }
            "udf-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::UDF { name })
            }
            _ => Err(kvapi::KeyError::InvalidSegment {
                i: p.index(),
                expect: "database-by-id|database-by-catalog-id|table-by-id|table-by-catalog-id|stage-by-name|udf-by-name"
                    .to_string(),
                got: q.to_string(),
            }),
        }
        //
    }
}

/// The meta-service key of object whose ownership to grant.
///
/// It could be a tenant's database, a tenant's table etc.
/// It is in form of `__fd_object_owners/<tenant>/<object>`.
/// where `<object>` could be:
/// - `database-by-id/<db_id>`
/// - `database-by-catalog-id/<catalog>/<db_id>`
/// - `table-by-id/<table_id>`
/// - `table-by-catalog-id/<catalog>/<table_id>`
/// - `stage-by-name/<stage_name>`
/// - `udf-by-name/<udf_name>`
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantOwnershipObject {
    tenant: Tenant,
    object: OwnershipObject,
}

impl TenantOwnershipObject {
    pub fn new(tenant: Tenant, subject: OwnershipObject) -> Self {
        // Legacy issue: Assert compatibility: No other catalog should be used.
        // Assertion is disabled.
        // Instead, accessing field `object` is disallowed.
        // match &subject {
        //     OwnershipObject::Database { catalog_name, .. } => {
        //         assert_eq!(catalog_name, OwnershipObject::DEFAULT_CATALOG);
        //     }
        //     OwnershipObject::Table { catalog_name, .. } => {
        //         assert_eq!(catalog_name, OwnershipObject::DEFAULT_CATALOG);
        //     }
        //     OwnershipObject::Stage { .. } => {}
        //     OwnershipObject::UDF { .. } => {}
        // }

        Self::new_unchecked(tenant, subject)
    }

    pub(crate) fn new_unchecked(tenant: Tenant, object: OwnershipObject) -> Self {
        TenantOwnershipObject { tenant, object }
    }

    // Because the encoded string key does not contain all of the field:
    // Catalog and db_id is missing, so we can not rebuild the complete key from string key.
    // Thus it is not allow to access this field.
    // pub fn subject(&self) -> &OwnershipObject {
    //     &self.object
    // }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum GrantObject {
    Global,
    Database(String, String),
    DatabaseById(String, u64),
    Table(String, String, String),
    TableById(String, u64, u64),
    UDF(String),
    Stage(String),
}

impl GrantObject {
    /// Comparing the grant objects, the Database object contains all the Table objects inside it.
    /// Global object contains all the Database objects.
    pub fn contains(&self, object: &GrantObject) -> bool {
        match (self, object) {
            (GrantObject::Global, _) => true,
            (GrantObject::Database(_, _), GrantObject::Global) => false,
            (GrantObject::Database(lcat, ldb), GrantObject::Database(rcat, rdb)) => {
                lcat == rcat && ldb == rdb
            }
            (GrantObject::DatabaseById(lcat, ldb), GrantObject::DatabaseById(rcat, rdb)) => {
                lcat == rcat && ldb == rdb
            }
            (GrantObject::DatabaseById(lcat, ldb), GrantObject::TableById(rcat, rdb, _)) => {
                lcat == rcat && ldb == rdb
            }
            (GrantObject::Database(lcat, ldb), GrantObject::Table(rcat, rdb, _)) => {
                lcat == rcat && ldb == rdb
            }
            (
                GrantObject::Table(lcat, lhs_db, lhs_table),
                GrantObject::Table(rcat, rhs_db, rhs_table),
            ) => lcat == rcat && (lhs_db == rhs_db) && (lhs_table == rhs_table),
            (
                GrantObject::TableById(lcat, lhs_db, lhs_table),
                GrantObject::TableById(rcat, rhs_db, rhs_table),
            ) => lcat == rcat && (lhs_db == rhs_db) && (lhs_table == rhs_table),
            (GrantObject::Table(_, _, _), _) => false,
            (GrantObject::Stage(lstage), GrantObject::Stage(rstage)) => lstage == rstage,
            (GrantObject::UDF(udf), GrantObject::UDF(rudf)) => udf == rudf,
            _ => false,
        }
    }

    /// Global, database and table has different available privileges
    pub fn available_privileges(&self, available_ownership: bool) -> UserPrivilegeSet {
        match self {
            GrantObject::Global => UserPrivilegeSet::available_privileges_on_global(),
            GrantObject::Database(_, _) | GrantObject::DatabaseById(_, _) => {
                UserPrivilegeSet::available_privileges_on_database(available_ownership)
            }
            GrantObject::Table(_, _, _) | GrantObject::TableById(_, _, _) => {
                UserPrivilegeSet::available_privileges_on_table(available_ownership)
            }
            GrantObject::UDF(_) => {
                UserPrivilegeSet::available_privileges_on_udf(available_ownership)
            }
            GrantObject::Stage(_) => {
                UserPrivilegeSet::available_privileges_on_stage(available_ownership)
            }
        }
    }

    pub fn catalog(&self) -> Option<String> {
        match self {
            GrantObject::Global | GrantObject::Stage(_) | GrantObject::UDF(_) => None,
            GrantObject::Database(cat, _) | GrantObject::DatabaseById(cat, _) => Some(cat.clone()),
            GrantObject::Table(cat, _, _) | GrantObject::TableById(cat, _, _) => Some(cat.clone()),
        }
    }
}

impl fmt::Display for GrantObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            GrantObject::Global => write!(f, "*.*"),
            GrantObject::Database(ref cat, ref db) => write!(f, "'{}'.'{}'.*", cat, db),
            GrantObject::DatabaseById(ref cat, ref db) => write!(f, "'{}'.'{}'.*", cat, db),
            GrantObject::Table(ref cat, ref db, ref table) => {
                write!(f, "'{}'.'{}'.'{}'", cat, db, table)
            }
            GrantObject::TableById(ref cat, ref db, ref table) => {
                write!(f, "'{}'.'{}'.'{}'", cat, db, table)
            }
            GrantObject::UDF(udf) => write!(f, "UDF {udf}"),
            GrantObject::Stage(stage) => write!(f, "STAGE {stage}"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GrantEntry {
    object: GrantObject,
    privileges: BitFlags<UserPrivilegeType>,
}

impl GrantEntry {
    pub fn new(object: GrantObject, privileges: BitFlags<UserPrivilegeType>) -> Self {
        Self { object, privileges }
    }

    pub fn object(&self) -> &GrantObject {
        &self.object
    }

    pub fn privileges(&self) -> &BitFlags<UserPrivilegeType> {
        &self.privileges
    }

    pub fn verify_privilege(
        &self,
        object: &GrantObject,
        privileges: Vec<UserPrivilegeType>,
    ) -> bool {
        // the verified object should be smaller than the object inside my grant entry.
        if !self.object.contains(object) {
            return false;
        }

        let mut priv_set = UserPrivilegeSet::empty();
        for privilege in privileges {
            priv_set.set_privilege(privilege)
        }
        self.privileges.contains(BitFlags::from(priv_set))
    }

    pub fn matches_entry(&self, object: &GrantObject) -> bool {
        &self.object == object
    }

    pub fn has_all_available_privileges(&self) -> bool {
        let all_available_privileges = self.object.available_privileges(false);
        self.privileges
            .contains(BitFlags::from(all_available_privileges))
    }
}

impl fmt::Display for GrantEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        let privileges: UserPrivilegeSet = self.privileges.into();
        let privileges_str = if self.has_all_available_privileges() {
            "ALL".to_string()
        } else {
            privileges.to_string()
        };
        write!(f, "GRANT {} ON {}", &privileges_str, self.object)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct UserGrantSet {
    entries: Vec<GrantEntry>,
    roles: HashSet<String>,
}

impl UserGrantSet {
    pub fn new(entries: Vec<GrantEntry>, roles: HashSet<String>) -> Self {
        UserGrantSet { entries, roles }
    }

    pub fn empty() -> Self {
        Self {
            entries: vec![],
            roles: HashSet::new(),
        }
    }

    pub fn entries(&self) -> Vec<GrantEntry> {
        self.entries.clone()
    }

    pub fn roles(&self) -> Vec<String> {
        self.roles.iter().cloned().collect::<Vec<_>>()
    }

    pub fn grant_role(&mut self, role: String) {
        self.roles.insert(role);
    }

    pub fn revoke_role(&mut self, role: &String) {
        self.roles.remove(role);
    }

    pub fn verify_privilege(
        &self,
        object: &GrantObject,
        privilege: Vec<UserPrivilegeType>,
    ) -> bool {
        self.entries
            .iter()
            .any(|e| e.verify_privilege(object, privilege.clone()))
    }

    pub fn grant_privileges(&mut self, object: &GrantObject, privileges: UserPrivilegeSet) {
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
            new_entries.push(GrantEntry::new(object.clone(), privileges))
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
                    e.privileges = e
                        .privileges
                        .iter()
                        .filter(|p| !privileges.contains(*p))
                        .collect();
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

impl ops::BitOrAssign for UserGrantSet {
    fn bitor_assign(&mut self, other: Self) {
        for entry in other.entries() {
            self.grant_privileges(&entry.object, entry.privileges.into());
        }
        for role in other.roles() {
            self.grant_role(role);
        }
    }
}

impl ops::BitOr for UserGrantSet {
    type Output = Self;
    fn bitor(self, other: Self) -> Self {
        let mut grants = self;
        grants |= other;
        grants
    }
}

impl fmt::Display for UserGrantSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        for entry in self.entries.iter() {
            write!(f, "{}, ", entry)?;
        }
        write!(f, "ROLES: {:?}", self.roles())
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::principal::user_grant::TenantOwnershipObject;
    use crate::principal::OwnershipInfo;
    use crate::principal::OwnershipObject;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::Key for TenantOwnershipObject {
        const PREFIX: &'static str = "__fd_object_owners";
        type ValueType = OwnershipInfo;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn to_string_key(&self) -> String {
            let b = kvapi::KeyBuilder::new_prefixed(Self::PREFIX).push_str(&self.tenant.tenant);
            self.object.build_key(b).done()
        }

        fn from_str_key(s: &str) -> Result<Self, KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_nonempty()?;
            let subject = OwnershipObject::parse_key(&mut p)?;
            p.done()?;

            Ok(TenantOwnershipObject {
                tenant: Tenant::new_nonempty(tenant),
                object: subject,
            })
        }
    }

    impl kvapi::Value for OwnershipInfo {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl KeyWithTenant for TenantOwnershipObject {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::user_grant::TenantOwnershipObject;
    use crate::principal::OwnershipObject;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    #[test]
    fn test_tenant_ownership_object_tenant_prefix() {
        let r =
            TenantOwnershipObject::new(Tenant::new_literal("tenant"), OwnershipObject::Database {
                catalog_name: "cat".to_string(),
                db_id: 1,
            });

        assert_eq!("__fd_object_owners/tenant/", r.tenant_prefix());
    }

    #[test]
    fn test_role_grantee_as_kvapi_key() {
        // db with default catalog
        {
            let role_grantee = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Database {
                    catalog_name: "default".to_string(),
                    db_id: 1,
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/database-by-id/1", key);

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // db with catalog
        {
            let role_grantee = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Database {
                    catalog_name: "cata/foo".to_string(),
                    db_id: 1,
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!(
                "__fd_object_owners/test/database-by-catalog-id/cata%2ffoo/1",
                key
            );

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // table with default catalog
        {
            let obj = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Table {
                    catalog_name: "default".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
            );

            let key = obj.to_string_key();
            assert_eq!("__fd_object_owners/test/table-by-id/2", key);

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(
                TenantOwnershipObject::new_unchecked(
                    Tenant::new_literal("test"),
                    OwnershipObject::Table {
                        catalog_name: "default".to_string(),
                        db_id: 0, // db_id is not encoded into key
                        table_id: 2,
                    }
                ),
                parsed
            );
        }

        // table with catalog
        {
            let obj = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Table {
                    catalog_name: "cata/foo".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
            );

            let key = obj.to_string_key();
            assert_eq!(
                "__fd_object_owners/test/table-by-catalog-id/cata%2ffoo/2",
                key
            );

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(
                TenantOwnershipObject::new_unchecked(
                    Tenant::new_literal("test"),
                    OwnershipObject::Table {
                        catalog_name: "cata/foo".to_string(),
                        db_id: 0, // db_id is not encoded into key
                        table_id: 2,
                    }
                ),
                parsed
            );
        }

        // stage
        {
            let role_grantee = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::Stage {
                    name: "foo".to_string(),
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/stage-by-name/foo", key);

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }

        // udf
        {
            let role_grantee = TenantOwnershipObject::new_unchecked(
                Tenant::new_literal("test"),
                OwnershipObject::UDF {
                    name: "foo".to_string(),
                },
            );

            let key = role_grantee.to_string_key();
            assert_eq!("__fd_object_owners/test/udf-by-name/foo", key);

            let parsed = TenantOwnershipObject::from_str_key(&key).unwrap();
            assert_eq!(role_grantee, parsed);
        }
    }
}

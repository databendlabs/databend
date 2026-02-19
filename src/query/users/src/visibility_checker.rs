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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_hashtable::HashMap as FastHashMap;
use databend_common_hashtable::HashSet as FastHashSet;
use databend_common_hashtable::HashtableKeyable;
use databend_common_meta_app::principal::GrantEntry;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::BitFlags;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;

pub enum Object {
    Stage,
    UDF,
    Warehouse,
    Connection,
    Sequence,
    Procedure,
    All,
}

struct CatalogIdPool {
    name_to_id: FxHashMap<Arc<str>, u32>,
    id_to_name: Vec<Arc<str>>,
}

impl CatalogIdPool {
    /// Creates a new empty catalog ID pool.
    fn new() -> Self {
        Self {
            name_to_id: FxHashMap::default(),
            id_to_name: Vec::new(),
        }
    }

    /// Gets or inserts a catalog name, returning its u32 ID.
    /// If the name already exists, returns the existing ID.
    /// If the name is new, assigns a new sequential ID.
    fn get_or_insert(&mut self, name: impl AsRef<str>) -> u32 {
        let name = name.as_ref();
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }

        let id = self.id_to_name.len() as u32;
        let name: Arc<str> = Arc::from(name);
        self.name_to_id.insert(name.clone(), id);
        self.id_to_name.push(name);
        id
    }

    /// Gets the catalog ID for a given name, if it exists.
    #[inline]
    fn get_id(&self, name: &str) -> Option<u32> {
        self.name_to_id.get(name).copied()
    }

    /// Gets the catalog name for a given ID.
    #[inline]
    fn get_name(&self, id: u32) -> Option<&Arc<str>> {
        self.id_to_name.get(id as usize)
    }
}

#[inline(always)]
fn fast_map_get_or_insert<K, V, F>(map: &mut FastHashMap<K, V>, key: K, ctor: F) -> &mut V
where
    K: HashtableKeyable,
    F: FnOnce() -> V,
{
    unsafe {
        match map.insert(key) {
            Ok(slot) => slot.write(ctor()),
            Err(entry) => entry,
        }
    }
}

#[inline(always)]
fn is_system_database(name: &str) -> bool {
    name.eq_ignore_ascii_case("information_schema") || name.eq_ignore_ascii_case("system")
}

/// Check if a grant entry only has USAGE privilege.
#[inline(always)]
fn is_usage_only(grant_entry: &GrantEntry) -> bool {
    grant_entry.privileges().len() == 1
        && grant_entry
            .privileges()
            .contains(BitFlags::from(UserPrivilegeType::Usage))
}

/// Check if user is owner based on owner_role and effective_roles.
#[inline]
pub fn is_role_owner(owner_role: Option<&str>, effective_roles: &[RoleInfo]) -> bool {
    owner_role.is_some_and(|role| effective_roles.iter().any(|r| r.name == role))
}

/// Checks visibility of grant objects (databases, tables, UDFs, stages, etc.) for a user and their roles.
///
/// This structure is optimized for large-scale ownership checking by:
/// 1. Using a CatalogIdPool to map catalog names to u32 IDs, avoiding repeated string hashing
/// 2. Using FastHashMap/FastHashSet for fast integer-based lookups
/// 3. Pre-allocating capacity to avoid hash table expansion
/// 4. Single-pass construction that traverses grant_sets and ownership_objects only once each
pub struct GrantObjectVisibilityChecker {
    // Global flags for unrestricted access
    granted_global_udf: bool,
    granted_global_ws: bool,
    granted_global_c: bool,
    granted_global_seq: bool,
    granted_global_procedure: bool,
    granted_global_masking_policy: bool,
    granted_global_stage: bool,
    granted_global_read_stage: bool,
    granted_global_db_table: bool,
    granted_global_row_access_policy: bool,

    // Catalog ID pool for efficient name → ID mapping
    catalog_pool: CatalogIdPool,

    // ID-based storage (u32 catalog_id → u64 snowflake_ids)
    // These use integer keys for fast lookups
    granted_databases_id: FastHashMap<u32, FastHashSet<u64>>,
    granted_tables_id: FastHashMap<u32, FastHashMap<u64, FastHashSet<u64>>>,
    extra_databases_id: FastHashMap<u32, FastHashSet<u64>>,
    granted_procedures_id: FastHashSet<u64>,
    granted_masking_policies_id: FastHashSet<u64>,
    granted_row_access_policies_id: FastHashSet<u64>,

    // Name-based storage (backward compatibility)
    granted_databases: FxHashSet<(Arc<str>, Arc<str>)>,
    extra_databases: FxHashSet<(Arc<str>, Arc<str>)>,
    granted_tables: FxHashSet<(Arc<str>, Arc<str>, Arc<str>)>,
    sys_databases: FxHashSet<(Arc<str>, Arc<str>)>,

    // Other name-based grants
    granted_udfs: FxHashSet<Arc<str>>,
    granted_write_stages: FxHashSet<Arc<str>>,
    granted_read_stages: FxHashSet<Arc<str>>,
    granted_ws: FxHashSet<Arc<str>>,
    granted_c: FxHashSet<Arc<str>>,
    granted_seq: FxHashSet<Arc<str>>,
}

/// Check if a table is visible based on user and roles grants (without ownership info).
/// This is a lightweight check that avoids loading all ownerships.
/// Returns true if the table is visible through grants.
#[inline]
pub fn check_table_visibility_with_roles(
    user: &UserInfo,
    roles: &[RoleInfo],
    catalog: &str,
    db_name: &str,
    db_id: u64,
    table_id: u64,
) -> bool {
    // System databases are always visible
    if is_system_database(db_name) {
        return true;
    }

    let grant_sets = std::iter::once(&user.grants).chain(roles.iter().map(|r| &r.grants));

    for grant_set in grant_sets {
        for grant_entry in grant_set.entries() {
            match grant_entry.object() {
                GrantObject::Global => {
                    // Check if has any database/table related privilege
                    if grant_entry.privileges().iter().any(|p| {
                        UserPrivilegeSet::available_privileges_on_database(false).has_privilege(p)
                    }) {
                        return true;
                    }
                }
                GrantObject::DatabaseById(cat, id) if cat == catalog && *id == db_id => {
                    if !is_usage_only(grant_entry) {
                        return true;
                    }
                }
                GrantObject::Database(cat, db) if cat == catalog && db == db_name => {
                    if !is_usage_only(grant_entry) {
                        return true;
                    }
                }
                GrantObject::TableById(cat, did, tid)
                    if cat == catalog && *did == db_id && *tid == table_id =>
                {
                    return true;
                }
                GrantObject::Table(cat, db, _table) if cat == catalog && db == db_name => {
                    // Name-based table grants are intentionally ignored here because this
                    // fast path does not know the target table name. Callers must ensure
                    // no name-based grants exist (see `has_table_name_grants`) or use a
                    // slower path that matches by table name.
                }
                _ => {}
            }
        }
    }

    false
}

/// Fast check: return true if any grant uses table name instead of table id.
#[inline]
pub fn has_table_name_grants(user: &UserInfo, roles: &[RoleInfo]) -> bool {
    let grant_sets = std::iter::once(&user.grants).chain(roles.iter().map(|r| &r.grants));

    for grant_set in grant_sets {
        for grant_entry in grant_set.entries() {
            if matches!(grant_entry.object(), GrantObject::Table(_, _, _)) {
                return true;
            }
        }
    }

    false
}

/// Check table visibility with ownership and grants.
#[inline]
pub fn is_table_visible_with_owner(
    user: &UserInfo,
    roles: &[RoleInfo],
    owner_role: Option<&str>,
    catalog: &str,
    db_name: &str,
    db_id: u64,
    table_id: u64,
) -> bool {
    is_role_owner(owner_role, roles)
        || check_table_visibility_with_roles(user, roles, catalog, db_name, db_id, table_id)
}

impl GrantObjectVisibilityChecker {
    pub fn new(user: &UserInfo, roles: &[RoleInfo], ownership_objects: &[OwnershipInfo]) -> Self {
        let mut granted_global_udf = false;
        let mut granted_global_ws = false;
        let mut granted_global_c = false;
        let mut granted_global_seq = false;
        let mut granted_global_procedure = false;
        let mut granted_global_stage = false;
        let mut granted_global_read_stage = false;
        let mut granted_global_db_table = false;
        let mut granted_global_masking_policy = false;
        let mut granted_global_row_access_policy = false;
        let mut catalog_pool = CatalogIdPool::new();
        let total_objects = ownership_objects.len();
        // Most deployments use only the default catalog
        let estimated_catalogs = 1;
        let estimated_dbs_per_catalog = (total_objects / estimated_catalogs / 10).max(16);
        // Adaptive initial capacity based on total objects:
        // - Small datasets (< 10K): 16 (minimal memory overhead)
        // - Medium datasets (10K-1M): 64 (good balance)
        // - Large datasets (> 1M): 128 (reduce rehashing for large single-DB cases)
        let estimated_tables_per_db = if total_objects < 10_000 {
            16
        } else if total_objects < 1_000_000 {
            64
        } else {
            128
        };

        let mut granted_databases_id: FastHashMap<u32, FastHashSet<u64>> =
            FastHashMap::with_capacity(estimated_catalogs);
        let mut granted_tables_id: FastHashMap<u32, FastHashMap<u64, FastHashSet<u64>>> =
            FastHashMap::with_capacity(estimated_catalogs);
        let mut extra_databases_id: FastHashMap<u32, FastHashSet<u64>> =
            FastHashMap::with_capacity(estimated_catalogs);
        let mut granted_procedures_id: FastHashSet<u64> = FastHashSet::with_capacity(16);
        let mut granted_masking_policies_id: FastHashSet<u64> = FastHashSet::with_capacity(16);
        let mut granted_row_access_policies_id: FastHashSet<u64> = FastHashSet::with_capacity(16);

        let mut granted_databases: FxHashSet<(Arc<str>, Arc<str>)> =
            FxHashSet::with_capacity_and_hasher(total_objects / 10, Default::default());
        let mut extra_databases: FxHashSet<(Arc<str>, Arc<str>)> =
            FxHashSet::with_capacity_and_hasher(total_objects / 10, Default::default());
        let mut granted_tables: FxHashSet<(Arc<str>, Arc<str>, Arc<str>)> =
            FxHashSet::with_capacity_and_hasher(total_objects, Default::default());

        let mut granted_udfs: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut granted_write_stages: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut granted_read_stages: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut granted_ws: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut granted_c: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut granted_seq: FxHashSet<Arc<str>> = FxHashSet::default();

        let grant_sets = std::iter::once(&user.grants).chain(roles.iter().map(|r| &r.grants));

        // Helper function to check if privileges match a certain type
        fn check_privilege<T, F>(granted: &mut bool, mut iter: T, condition: F)
        where
            T: Iterator<Item = UserPrivilegeType>,
            F: Fn(UserPrivilegeType) -> bool,
        {
            if !*granted {
                *granted |= iter.any(condition);
            }
        }

        for grant_set in grant_sets {
            for grant_entry in grant_set.entries() {
                match grant_entry.object() {
                    GrantObject::Global => {
                        check_privilege(
                            &mut granted_global_udf,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_udf(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_ws,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_warehouse(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_c,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_connection(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_seq,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_sequence(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_procedure,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_procedure(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_masking_policy,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_masking_policy(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_row_access_policy,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_row_access_policy(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_stage,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_stage(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_read_stage,
                            grant_entry.privileges().iter(),
                            |privilege| privilege == UserPrivilegeType::Read,
                        );

                        check_privilege(
                            &mut granted_global_db_table,
                            grant_entry.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_database(false)
                                    .has_privilege(privilege)
                            },
                        );
                    }
                    GrantObject::DatabaseById(catalog, db_id) => {
                        // If only has db level usage privilege means only support use db.
                        if !(grant_entry.privileges().len() == 1
                            && grant_entry
                                .privileges()
                                .contains(BitFlags::from(UserPrivilegeType::Usage)))
                        {
                            let catalog_id = catalog_pool.get_or_insert(catalog);
                            let db_set = fast_map_get_or_insert(
                                &mut granted_databases_id,
                                catalog_id,
                                || FastHashSet::with_capacity(estimated_dbs_per_catalog),
                            );
                            let _ = db_set.set_insert(*db_id);
                        }
                    }
                    GrantObject::Database(catalog, db) => {
                        // If only has db level usage privilege means only support use db.
                        if !(grant_entry.privileges().len() == 1
                            && grant_entry
                                .privileges()
                                .contains(BitFlags::from(UserPrivilegeType::Usage)))
                        {
                            let catalog: Arc<str> = Arc::from(catalog.as_str());
                            let db: Arc<str> = Arc::from(db.as_str());
                            granted_databases.insert((catalog, db));
                        }
                    }
                    GrantObject::TableById(catalog, db_id, table_id) => {
                        let catalog_id = catalog_pool.get_or_insert(catalog);
                        let db_map =
                            fast_map_get_or_insert(&mut granted_tables_id, catalog_id, || {
                                FastHashMap::with_capacity(estimated_dbs_per_catalog)
                            });
                        let table_set = fast_map_get_or_insert(db_map, *db_id, || {
                            FastHashSet::with_capacity(estimated_tables_per_db)
                        });
                        let _ = table_set.set_insert(*table_id);

                        // if table is visible, the table's database is also treated as visible
                        let extra_set = fast_map_get_or_insert(
                            &mut extra_databases_id,
                            catalog_pool.get_or_insert(catalog),
                            || FastHashSet::with_capacity(estimated_dbs_per_catalog),
                        );
                        let _ = extra_set.set_insert(*db_id);
                    }
                    GrantObject::Table(catalog, db, table) => {
                        let catalog: Arc<str> = Arc::from(catalog.as_str());
                        let db: Arc<str> = Arc::from(db.as_str());
                        let table: Arc<str> = Arc::from(table.as_str());
                        granted_tables.insert((catalog.clone(), db.clone(), table));

                        // if table is visible, the table's database is also treated as visible
                        extra_databases.insert((catalog, db));
                    }
                    GrantObject::UDF(udf) => {
                        granted_udfs.insert(Arc::from(udf.as_str()));
                    }
                    GrantObject::Stage(stage) => {
                        if grant_entry
                            .privileges()
                            .contains(BitFlags::from(UserPrivilegeType::Write))
                        {
                            granted_write_stages.insert(Arc::from(stage.as_str()));
                        }
                        if grant_entry
                            .privileges()
                            .contains(BitFlags::from(UserPrivilegeType::Read))
                        {
                            granted_read_stages.insert(Arc::from(stage.as_str()));
                        }
                    }
                    GrantObject::Warehouse(w) => {
                        granted_ws.insert(Arc::from(w.as_str()));
                    }
                    GrantObject::Connection(c) => {
                        granted_c.insert(Arc::from(c.as_str()));
                    }
                    GrantObject::Sequence(s) => {
                        granted_seq.insert(Arc::from(s.as_str()));
                    }
                    GrantObject::Procedure(procedure_id) => {
                        let _ = granted_procedures_id.set_insert(*procedure_id);
                    }
                    GrantObject::MaskingPolicy(policy_id) => {
                        let _ = granted_masking_policies_id.set_insert(*policy_id);
                    }
                    GrantObject::RowAccessPolicy(policy_id) => {
                        let _ = granted_row_access_policies_id.set_insert(*policy_id);
                    }
                }
            }
        }

        for ownership in ownership_objects {
            match &ownership.object {
                OwnershipObject::Database {
                    catalog_name,
                    db_id,
                } => {
                    let catalog_id = catalog_pool.get_or_insert(catalog_name);
                    let db_set =
                        fast_map_get_or_insert(&mut granted_databases_id, catalog_id, || {
                            FastHashSet::with_capacity(estimated_dbs_per_catalog)
                        });
                    let _ = db_set.set_insert(*db_id);
                    let extra_set =
                        fast_map_get_or_insert(&mut extra_databases_id, catalog_id, || {
                            FastHashSet::with_capacity(estimated_dbs_per_catalog)
                        });
                    let _ = extra_set.set_insert(*db_id);
                }
                OwnershipObject::Table {
                    catalog_name,
                    db_id,
                    table_id,
                } => {
                    let catalog_id = catalog_pool.get_or_insert(catalog_name);
                    let db_map = fast_map_get_or_insert(&mut granted_tables_id, catalog_id, || {
                        FastHashMap::with_capacity(estimated_dbs_per_catalog)
                    });
                    let table_set = fast_map_get_or_insert(db_map, *db_id, || {
                        FastHashSet::with_capacity(estimated_tables_per_db)
                    });
                    let _ = table_set.set_insert(*table_id);

                    // if table is visible, the table's database is also treated as visible
                    let extra_set =
                        fast_map_get_or_insert(&mut extra_databases_id, catalog_id, || {
                            FastHashSet::with_capacity(estimated_dbs_per_catalog)
                        });
                    let _ = extra_set.set_insert(*db_id);
                }
                OwnershipObject::Stage { name } => {
                    let name: Arc<str> = Arc::from(name.as_str());
                    granted_write_stages.insert(name.clone());
                    granted_read_stages.insert(name);
                }
                OwnershipObject::UDF { name } => {
                    granted_udfs.insert(Arc::from(name.as_str()));
                }
                OwnershipObject::Warehouse { id } => {
                    granted_ws.insert(Arc::from(id.as_str()));
                }
                OwnershipObject::Connection { name } => {
                    granted_c.insert(Arc::from(name.as_str()));
                }
                OwnershipObject::Sequence { name } => {
                    granted_seq.insert(Arc::from(name.as_str()));
                }
                OwnershipObject::Procedure { procedure_id } => {
                    let _ = granted_procedures_id.set_insert(*procedure_id);
                }
                OwnershipObject::MaskingPolicy { policy_id } => {
                    let _ = granted_masking_policies_id.set_insert(*policy_id);
                }
                OwnershipObject::RowAccessPolicy { policy_id } => {
                    let _ = granted_row_access_policies_id.set_insert(*policy_id);
                }
            }
        }

        // Phase 4: Add system databases
        let mut sys_databases: FxHashSet<(Arc<str>, Arc<str>)> =
            FxHashSet::with_capacity_and_hasher(2, Default::default());
        let default_catalog: Arc<str> = Arc::from("default");
        let info_schema: Arc<str> = Arc::from("information_schema");
        let system: Arc<str> = Arc::from("system");
        sys_databases.insert((default_catalog.clone(), info_schema));
        sys_databases.insert((default_catalog, system));

        Self {
            granted_global_udf,
            granted_global_ws,
            granted_global_c,
            granted_global_seq,
            granted_global_procedure,
            granted_global_masking_policy,
            granted_global_row_access_policy,
            granted_global_stage,
            granted_global_read_stage,
            granted_global_db_table,
            catalog_pool,
            granted_databases_id,
            granted_tables_id,
            extra_databases_id,
            granted_databases,
            extra_databases,
            granted_tables,
            sys_databases,
            granted_udfs,
            granted_write_stages,
            granted_read_stages,
            granted_ws,
            granted_c,
            granted_seq,
            granted_procedures_id,
            granted_masking_policies_id,
            granted_row_access_policies_id,
        }
    }

    #[inline(always)]
    pub fn check_database_visibility_by_id(&self, catalog_id: u32, db_id: u64) -> bool {
        if self.granted_global_db_table {
            return true;
        }

        if self
            .granted_databases_id
            .get(&catalog_id)
            .map(|set| set.contains(&db_id))
            .unwrap_or(false)
        {
            return true;
        }

        if self
            .extra_databases_id
            .get(&catalog_id)
            .map(|set| set.contains(&db_id))
            .unwrap_or(false)
        {
            return true;
        }

        false
    }

    #[inline(always)]
    pub fn check_table_visibility_by_id(&self, catalog_id: u32, db_id: u64, table_id: u64) -> bool {
        if self.granted_global_db_table {
            return true;
        }

        if self
            .granted_databases_id
            .get(&catalog_id)
            .map(|set| set.contains(&db_id))
            .unwrap_or(false)
        {
            return true;
        }

        if let Some(db_tables) = self.granted_tables_id.get(&catalog_id) {
            if let Some(tables) = db_tables.get(&db_id) {
                if tables.contains(&table_id) {
                    return true;
                }
            }
        }

        false
    }

    #[inline]
    pub fn check_database_visibility(&self, catalog: &str, db: &str, db_id: u64) -> bool {
        // Fast path: global privileges
        if self.granted_global_db_table {
            return true;
        }

        // System databases in every catalog are always visible
        if is_system_database(db) {
            return true;
        }

        if let Some(catalog_id) = self.catalog_pool.get_id(catalog) {
            if self.check_database_visibility_by_id(catalog_id, db_id) {
                return true;
            }
        }

        if self.granted_databases.is_empty()
            && self.extra_databases.is_empty()
            && self.sys_databases.is_empty()
        {
            return false;
        }

        let catalog: Arc<str> = Arc::from(catalog);
        let db: Arc<str> = Arc::from(db);
        if self.sys_databases.contains(&(catalog.clone(), db.clone())) {
            return true;
        }

        if self
            .extra_databases
            .contains(&(catalog.clone(), db.clone()))
        {
            return true;
        }

        self.granted_databases.contains(&(catalog, db))
    }

    #[inline]
    pub fn check_table_visibility(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        db_id: u64,
        table_id: u64,
    ) -> bool {
        // Skip system databases
        if is_system_database(database) {
            return true;
        }

        if self.granted_global_db_table {
            return true;
        }

        if let Some(catalog_id) = self.catalog_pool.get_id(catalog) {
            if self.check_table_visibility_by_id(catalog_id, db_id, table_id) {
                return true;
            }
        }

        if self.granted_databases.is_empty() && self.granted_tables.is_empty() {
            return false;
        }

        let catalog: Arc<str> = Arc::from(catalog);
        let database: Arc<str> = Arc::from(database);

        if self
            .granted_databases
            .contains(&(catalog.clone(), database.clone()))
        {
            return true;
        }

        let table: Arc<str> = Arc::from(table);
        if self.granted_tables.contains(&(catalog, database, table)) {
            return true;
        }

        false
    }

    #[inline]
    pub fn check_udf_visibility(&self, udf: &str) -> bool {
        if self.granted_global_udf {
            return true;
        }
        if self.granted_udfs.contains(udf) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_stage_visibility(&self, stage: &str) -> bool {
        if self.granted_global_stage {
            return true;
        }
        if self.granted_write_stages.contains(stage) {
            return true;
        }
        if self.granted_read_stages.contains(stage) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_stage_read_visibility(&self, stage: &str) -> bool {
        if self.granted_global_read_stage {
            return true;
        }
        if self.granted_read_stages.contains(stage) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_warehouse_visibility(&self, id: &str) -> bool {
        if self.granted_global_ws {
            return true;
        }
        if self.granted_ws.contains(id) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_connection_visibility(&self, name: &str) -> bool {
        if self.granted_global_c {
            return true;
        }
        if self.granted_c.contains(name) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_seq_visibility(&self, name: &str) -> bool {
        if self.granted_global_seq {
            return true;
        }
        if self.granted_seq.contains(name) {
            return true;
        }
        false
    }

    #[inline]
    pub fn check_procedure_visibility(&self, id: &u64) -> bool {
        if self.granted_global_procedure {
            return true;
        }
        if self.granted_procedures_id.contains(id) {
            return true;
        }
        false
    }

    #[inline(always)]
    pub fn check_masking_policy_visibility(&self, id: &u64) -> bool {
        if self.granted_global_masking_policy {
            return true;
        }
        self.granted_masking_policies_id.contains(id)
    }

    #[inline(always)]
    pub fn check_row_access_policy_visibility(&self, id: &u64) -> bool {
        if self.granted_global_row_access_policy {
            return true;
        }
        self.granted_row_access_policies_id.contains(id)
    }

    #[allow(clippy::type_complexity)]
    pub fn get_visibility_database(
        &self,
    ) -> Option<HashMap<&str, HashSet<(Option<&str>, Option<&u64>)>>> {
        if self.granted_global_db_table {
            return None;
        }

        let capacity = self.granted_databases.len()
            + self.extra_databases.len()
            + self.granted_databases_id.len()
            + self.extra_databases_id.len()
            + self.sys_databases.len();

        let dbs = self
            .granted_databases
            .iter()
            .map(|(catalog, db)| (catalog.as_ref(), (Some(db.as_ref()), None)))
            .chain(
                self.extra_databases
                    .iter()
                    .map(|(catalog, db)| (catalog.as_ref(), (Some(db.as_ref()), None))),
            )
            .chain(
                self.granted_databases_id
                    .iter()
                    .flat_map(|entry| {
                        let catalog_id = *entry.key();
                        self.catalog_pool.get_name(catalog_id).map(|catalog| {
                            entry.get().iter().map(move |db_entry| {
                                (catalog.as_ref(), (None, Some(db_entry.key())))
                            })
                        })
                    })
                    .flatten(),
            )
            .chain(
                self.extra_databases_id
                    .iter()
                    .flat_map(|entry| {
                        let catalog_id = *entry.key();
                        self.catalog_pool.get_name(catalog_id).map(|catalog| {
                            entry.get().iter().map(move |db_entry| {
                                (catalog.as_ref(), (None, Some(db_entry.key())))
                            })
                        })
                    })
                    .flatten(),
            )
            .chain(
                self.sys_databases
                    .iter()
                    .map(|(catalog, db)| (catalog.as_ref(), (Some(db.as_ref()), None))),
            )
            .into_grouping_map()
            .fold(
                HashSet::with_capacity(capacity / 4),
                |mut set, _key, value| {
                    set.insert(value);
                    set
                },
            );
        Some(dbs)
    }
}

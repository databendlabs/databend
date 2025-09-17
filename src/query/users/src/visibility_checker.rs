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
use std::string::ToString;

use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::BitFlags;
use itertools::Itertools;

pub enum Object {
    Stage,
    UDF,
    Warehouse,
    Connection,
    Sequence,
    All,
}

/// GrantObjectVisibilityChecker is used to check whether a user has the privilege to access a
/// database or table.
/// It is used in `SHOW DATABASES` and `SHOW TABLES` statements.
pub struct GrantObjectVisibilityChecker {
    granted_global_udf: bool,
    granted_global_ws: bool,
    granted_global_c: bool,
    granted_global_seq: bool,
    granted_global_procedure: bool,
    granted_global_db_table: bool,
    granted_global_stage: bool,
    granted_global_read_stage: bool,
    granted_databases: HashSet<(String, String)>,
    granted_databases_id: HashSet<(String, u64)>,
    granted_tables: HashSet<(String, String, String)>,
    granted_tables_id: HashSet<(String, u64, u64)>,
    extra_databases: HashSet<(String, String)>,
    sys_databases: HashSet<(String, String)>,
    extra_databases_id: HashSet<(String, u64)>,
    granted_udfs: HashSet<String>,
    granted_write_stages: HashSet<String>,
    granted_read_stages: HashSet<String>,
    granted_ws: HashSet<String>,
    granted_c: HashSet<String>,
    granted_seq: HashSet<String>,
    granted_procedures_id: HashSet<u64>,
}

impl GrantObjectVisibilityChecker {
    pub fn new(
        user: &UserInfo,
        available_roles: &Vec<RoleInfo>,
        ownership_objects: &[OwnershipObject],
    ) -> Self {
        let mut granted_global_udf = false;
        let mut granted_global_ws = false;
        let mut granted_global_c = false;
        let mut granted_global_seq = false;
        let mut granted_global_procedure = false;
        let mut granted_global_db_table = false;
        let mut granted_global_stage = false;
        let mut granted_global_read_stage = false;
        let mut granted_databases = HashSet::new();
        let mut granted_tables = HashSet::new();
        let mut granted_udfs = HashSet::new();
        let mut granted_ws = HashSet::new();
        let mut granted_c = HashSet::new();
        let mut granted_seq = HashSet::new();
        let mut granted_procedures_id = HashSet::new();
        let mut granted_write_stages = HashSet::new();
        let mut granted_read_stages = HashSet::new();
        let mut extra_databases = HashSet::new();
        let mut granted_databases_id = HashSet::new();
        let mut extra_databases_id = HashSet::new();
        let mut granted_tables_id = HashSet::new();

        let mut grant_sets: Vec<&UserGrantSet> = vec![&user.grants];
        for role in available_roles {
            grant_sets.push(&role.grants);
        }
        for grant_set in grant_sets {
            for ent in grant_set.entries() {
                match ent.object() {
                    GrantObject::Global => {
                        // this check validates every granted entry's privileges
                        // contains any privilege on *.* of database/table/stage/udf
                        fn check_privilege<T, F>(granted: &mut bool, mut iter: T, condition: F)
                        where
                            T: Iterator<Item = UserPrivilegeType>,
                            F: Fn(UserPrivilegeType) -> bool,
                        {
                            if !*granted {
                                *granted |= iter.any(condition);
                            }
                        }

                        check_privilege(
                            &mut granted_global_udf,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_udf(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_ws,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_warehouse(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_c,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_connection(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_seq,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_sequence(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_procedure,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_procedure(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_stage,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_stage(false)
                                    .has_privilege(privilege)
                            },
                        );

                        check_privilege(
                            &mut granted_global_read_stage,
                            ent.privileges().iter(),
                            |privilege| privilege == UserPrivilegeType::Read,
                        );

                        check_privilege(
                            &mut granted_global_db_table,
                            ent.privileges().iter(),
                            |privilege| {
                                UserPrivilegeSet::available_privileges_on_database(false)
                                    .has_privilege(privilege)
                            },
                        );
                    }
                    GrantObject::Database(catalog, db) => {
                        if !(ent.privileges().len() == 1
                            && ent.privileges().contains(UserPrivilegeType::Usage))
                        {
                            granted_databases.insert((catalog.to_string(), db.to_string()));
                        }
                    }
                    GrantObject::DatabaseById(catalog, db) => {
                        // If only has db level usage privilege means only support use db.
                        if !(ent.privileges().len() == 1
                            && ent.privileges().contains(UserPrivilegeType::Usage))
                        {
                            granted_databases_id.insert((catalog.to_string(), *db));
                        }
                    }
                    GrantObject::Table(catalog, db, table) => {
                        granted_tables.insert((
                            catalog.to_string(),
                            db.to_string(),
                            table.to_string(),
                        ));
                        // if table is visible, the table's database is also treated as visible
                        extra_databases.insert((catalog.to_string(), db.to_string()));
                    }
                    GrantObject::TableById(catalog, db, table) => {
                        granted_tables_id.insert((catalog.to_string(), *db, *table));
                        // if table is visible, the table's database is also treated as visible
                        extra_databases_id.insert((catalog.to_string(), *db));
                    }
                    GrantObject::UDF(udf) => {
                        granted_udfs.insert(udf.to_string());
                    }
                    GrantObject::Stage(stage) => {
                        if ent
                            .privileges()
                            .contains(BitFlags::from(UserPrivilegeType::Write))
                        {
                            granted_write_stages.insert(stage.to_string());
                        }
                        if ent
                            .privileges()
                            .contains(BitFlags::from(UserPrivilegeType::Read))
                        {
                            granted_read_stages.insert(stage.to_string());
                        }
                    }
                    GrantObject::Warehouse(w) => {
                        granted_ws.insert(w.to_string());
                    }
                    GrantObject::Connection(c) => {
                        granted_c.insert(c.to_string());
                    }
                    GrantObject::Sequence(c) => {
                        granted_seq.insert(c.to_string());
                    }
                    GrantObject::Procedure(p_id) => {
                        granted_procedures_id.insert(*p_id);
                    }
                }
            }
        }

        for ownership_object in ownership_objects {
            match ownership_object {
                OwnershipObject::Database {
                    catalog_name,
                    db_id,
                } => {
                    granted_databases_id.insert((catalog_name.to_string(), *db_id));
                }
                OwnershipObject::Table {
                    catalog_name,
                    db_id,
                    table_id,
                } => {
                    granted_tables_id.insert((catalog_name.to_string(), *db_id, *table_id));
                    // if table is visible, the table's database is also treated as visible
                    extra_databases_id.insert((catalog_name.to_string(), *db_id));
                }
                OwnershipObject::Stage { name } => {
                    granted_write_stages.insert(name.to_string());
                    granted_read_stages.insert(name.to_string());
                }
                OwnershipObject::UDF { name } => {
                    granted_udfs.insert(name.to_string());
                }
                OwnershipObject::Warehouse { id } => {
                    granted_ws.insert(id.to_string());
                }
                OwnershipObject::Connection { name } => {
                    granted_c.insert(name.to_string());
                }
                OwnershipObject::Sequence { name } => {
                    granted_seq.insert(name.to_string());
                }
                OwnershipObject::Procedure { p_id } => {
                    granted_procedures_id.insert(*p_id);
                }
            }
        }

        Self {
            granted_global_udf,
            granted_global_ws,
            granted_global_c,
            granted_global_seq,
            granted_global_procedure,
            granted_global_db_table,
            granted_global_stage,
            granted_global_read_stage,
            granted_databases,
            granted_databases_id,
            granted_tables,
            granted_tables_id,
            extra_databases,
            extra_databases_id,
            granted_udfs,
            granted_write_stages,
            granted_read_stages,
            sys_databases: HashSet::from([
                ("default".to_string(), "information_schema".to_string()),
                ("default".to_string(), "system".to_string()),
            ]),
            granted_ws,
            granted_c,
            granted_seq,
            granted_procedures_id,
        }
    }

    pub fn check_stage_visibility(&self, stage: &str) -> bool {
        if self.granted_global_stage {
            return true;
        }

        if self.granted_read_stages.contains(stage) || self.granted_write_stages.contains(stage) {
            return true;
        }
        false
    }

    pub fn check_stage_read_visibility(&self, stage: &str) -> bool {
        if self.granted_global_read_stage {
            return true;
        }

        if self.granted_read_stages.contains(stage) {
            return true;
        }
        false
    }

    pub fn check_udf_visibility(&self, udf: &str) -> bool {
        if self.granted_global_udf {
            return true;
        }

        if self.granted_udfs.contains(udf) {
            return true;
        }
        false
    }

    pub fn check_warehouse_visibility(&self, id: &str) -> bool {
        if self.granted_global_ws {
            return true;
        }

        if self.granted_ws.contains(id) {
            return true;
        }
        false
    }

    pub fn check_procedure_visibility(&self, id: &u64) -> bool {
        if self.granted_global_procedure {
            return true;
        }

        if self.granted_procedures_id.contains(id) {
            return true;
        }
        false
    }

    pub fn check_connection_visibility(&self, name: &str) -> bool {
        if self.granted_global_c {
            return true;
        }
        if self.granted_c.contains(name) {
            return true;
        }
        false
    }

    pub fn check_seq_visibility(&self, name: &str) -> bool {
        if self.granted_global_seq {
            return true;
        }
        if self.granted_seq.contains(name) {
            return true;
        }
        false
    }

    pub fn check_database_visibility(&self, catalog: &str, db: &str, db_id: u64) -> bool {
        // skip information_schema privilege check
        if db.to_lowercase() == "information_schema" || db.to_lowercase() == "system" {
            return true;
        }

        if self.granted_global_db_table {
            return true;
        }

        if self
            .granted_databases
            .contains(&(catalog.to_string(), db.to_string()))
        {
            return true;
        }

        if self
            .granted_databases_id
            .contains(&(catalog.to_string(), db_id))
        {
            return true;
        }

        // if one of the tables in the database is granted, the database is also visible
        if self
            .extra_databases
            .contains(&(catalog.to_string(), db.to_string()))
        {
            return true;
        }

        if self
            .extra_databases_id
            .contains(&(catalog.to_string(), db_id))
        {
            return true;
        }

        false
    }

    pub fn check_table_visibility(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        db_id: u64,
        table_id: u64,
    ) -> bool {
        // skip information_schema privilege check
        if database.to_lowercase() == "information_schema" || database.to_lowercase() == "system" {
            return true;
        }

        if self.granted_global_db_table {
            return true;
        }

        // if database is granted, all the tables in it are visible
        if self
            .granted_databases
            .contains(&(catalog.to_string(), database.to_string()))
        {
            return true;
        }

        if self
            .granted_databases_id
            .contains(&(catalog.to_string(), db_id))
        {
            return true;
        }

        if self.granted_tables.contains(&(
            catalog.to_string(),
            database.to_string(),
            table.to_string(),
        )) {
            return true;
        }

        if self
            .granted_tables_id
            .contains(&(catalog.to_string(), db_id, table_id))
        {
            return true;
        }

        false
    }

    #[allow(clippy::type_complexity)]
    pub fn get_visibility_database(
        &self,
    ) -> Option<HashMap<&String, HashSet<(Option<&String>, Option<&u64>)>>> {
        if self.granted_global_db_table {
            return None;
        }

        let capacity = self.granted_databases.len()
            + self.granted_databases_id.len()
            + self.extra_databases.len()
            + self.extra_databases_id.len()
            + self.sys_databases.len();

        let dbs = self
            .granted_databases
            .iter()
            .map(|(catalog, db)| (catalog, (Some(db), None)))
            .chain(
                self.granted_databases_id
                    .iter()
                    .map(|(catalog, db_id)| (catalog, (None, Some(db_id)))),
            )
            .chain(
                self.extra_databases
                    .iter()
                    .map(|(catalog, db)| (catalog, (Some(db), None))),
            )
            .chain(
                self.sys_databases
                    .iter()
                    .map(|(catalog, db)| (catalog, (Some(db), None))),
            )
            .chain(
                self.extra_databases_id
                    .iter()
                    .map(|(catalog, db_id)| (catalog, (None, Some(db_id)))),
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

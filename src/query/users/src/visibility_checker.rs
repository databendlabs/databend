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

use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use enumflags2::BitFlags;

/// GrantObjectVisibilityChecker is used to check whether a user has the privilege to access a
/// database or table.
/// It is used in `SHOW DATABASES` and `SHOW TABLES` statements.
pub struct GrantObjectVisibilityChecker {
    granted_global_udf: bool,
    granted_global_db_table: bool,
    granted_global_stage: bool,
    granted_global_read_stage: bool,
    granted_databases: HashSet<(String, String)>,
    granted_databases_id: HashSet<(String, u64)>,
    granted_tables: HashSet<(String, String, String)>,
    granted_tables_id: HashSet<(String, u64, u64)>,
    extra_databases: HashSet<(String, String)>,
    extra_databases_id: HashSet<(String, u64)>,
    granted_udfs: HashSet<String>,
    granted_write_stages: HashSet<String>,
    granted_read_stages: HashSet<String>,
}

impl GrantObjectVisibilityChecker {
    pub fn new(user: &UserInfo, available_roles: &Vec<RoleInfo>) -> Self {
        let mut granted_global_udf = false;
        let mut granted_global_db_table = false;
        let mut granted_global_stage = false;
        let mut granted_global_read_stage = false;
        let mut granted_databases = HashSet::new();
        let mut granted_tables = HashSet::new();
        let mut granted_udfs = HashSet::new();
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
                        granted_databases.insert((catalog.to_string(), db.to_string()));
                    }
                    GrantObject::DatabaseById(catalog, db) => {
                        granted_databases_id.insert((catalog.to_string(), *db));
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
                    GrantObject::Task(_) => {}
                }
            }
        }

        Self {
            granted_global_udf,
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
}

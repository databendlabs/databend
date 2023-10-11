use std::collections::HashSet;

use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserGrantSet;
use common_meta_app::principal::UserInfo;

/// GrantObjectVisibilityChecker is used to check whether a user has the privilege to access a
/// database or table.
/// It is used in `SHOW DATABASES` and `SHOW TABLES` statements.
pub struct GrantObjectVisibilityChecker {
    granted_global: bool,
    granted_databases: HashSet<(String, String)>,
    granted_tables: HashSet<(String, String, String)>,
    extra_databases: HashSet<(String, String)>,
}

impl GrantObjectVisibilityChecker {
    pub fn new(user: &UserInfo, available_roles: &Vec<RoleInfo>) -> Self {
        let mut granted_global = false;
        let mut granted_databases = HashSet::new();
        let mut granted_tables = HashSet::new();
        let mut extra_databases = HashSet::new();

        let mut grant_sets: Vec<&UserGrantSet> = vec![&user.grants];
        for role in available_roles {
            grant_sets.push(&role.grants);
        }

        for grant_set in grant_sets {
            for ent in grant_set.entries() {
                match ent.object() {
                    GrantObject::Global => {
                        granted_global = true;
                    }
                    GrantObject::Database(catalog, db) => {
                        granted_databases.insert((catalog.to_string(), db.to_string()));
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
                }
            }
        }

        Self {
            granted_global,
            granted_databases,
            granted_tables,
            extra_databases,
        }
    }

    pub fn check_database_visibility(&self, catalog: &str, db: &str) -> bool {
        if self.granted_global {
            return true;
        }

        if self
            .granted_databases
            .contains(&(catalog.to_string(), db.to_string()))
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

        false
    }

    pub fn check_table_visibility(&self, catalog: &str, database: &str, table: &str) -> bool {
        if self.granted_global {
            return true;
        }

        // if database is granted, all the tables in it are visible
        if self
            .granted_databases
            .contains(&(catalog.to_string(), database.to_string()))
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

        false
    }
}

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

//! simply wrapping directories into databases

use common_catalog::database::Database;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;

#[derive(Clone, Debug)]
pub struct IcebergDatabase {
    info: DatabaseInfo,
}

impl IcebergDatabase {
    /// create an void database naming `default`
    ///
    /// *for flatten catalogs only*
    pub fn create_database_ommited_default(tenant: &str) -> Self {
        let info = DatabaseInfo {
            ident: DatabaseIdent { db_id: 0, seq: 0 },
            name_ident: DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: "default".to_string(),
            },
            meta: DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            },
        };
        Self { info }
    }
    /// create a new database, but from reading
    pub fn create_database_from_read(name: &str, tenant: &str) -> Self {
        let info = DatabaseInfo {
            ident: DatabaseIdent { db_id: 0, seq: 0 },
            name_ident: DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: name.to_string(),
            },
            meta: DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            },
        };
        Self { info }
    }
}

impl Database for IcebergDatabase {
    fn name(&self) -> &str {
        &self.info.name_ident.db_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.info
    }
}

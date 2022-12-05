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

//! Wrapping of the parent directory containing iceberg tables

use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::database::Database;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use opendal::layers::SubdirLayer;
use opendal::Operator;

use crate::table::IcebergTable;

#[derive(Clone, Debug)]
pub struct IcebergDatabase {
    /// catalog this database belongs to
    ctl_name: String,
    /// operator pointing to the directory holding iceberg tables
    db_root: Operator,
    /// database infomations
    info: DatabaseInfo,
}

impl IcebergDatabase {
    /// create an void database naming `default`
    ///
    /// *for flatten catalogs only*
    pub fn create_database_ommited_default(
        tenant: &str,
        ctl_name: &str,
        db_root: Operator,
    ) -> Self {
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
        Self {
            ctl_name: ctl_name.to_string(),
            db_root,
            info,
        }
    }
    /// create a new database, but from reading
    pub fn create_database_from_read(
        tenant: &str,
        ctl_name: &str,
        db_name: &str,
        db_root: Operator,
    ) -> Self {
        let info = DatabaseInfo {
            ident: DatabaseIdent { db_id: 0, seq: 0 },
            name_ident: DatabaseNameIdent {
                tenant: tenant.to_string(),
                db_name: db_name.to_string(),
            },
            meta: DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            },
        };
        Self {
            ctl_name: ctl_name.to_string(),
            db_root,
            info,
        }
    }
}

#[async_trait]
impl Database for IcebergDatabase {
    fn name(&self) -> &str {
        &self.info.name_ident.db_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.info
    }

    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let path = format!("{}/", table_name);
        // check existence first
        let tbl_obj = self.db_root.object(&path);
        if !tbl_obj.is_exist().await? {
            return Err(ErrorCode::UnknownTable(format!(
                "table {} does not exist",
                table_name
            )));
        }

        let tbl_root = self.db_root.clone().layer(SubdirLayer::new(&path));
        let tbl = IcebergTable::try_create_table_from_read(
            &self.ctl_name,
            &self.info.name_ident.tenant,
            &self.info.name_ident.db_name,
            table_name,
            tbl_root,
        )
        .await?;
        return Ok(Arc::new(tbl) as Arc<dyn Table>);
    }
}

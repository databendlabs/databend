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

//! 2022-11-25:
//! TODO: support synchronize with remote
//! Note:
//! currently, we only care about immutable tables
//! once the table created we don't update it.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use iceberg_rs::model::table::TableMetadataV2;
use opendal::Operator;

use crate::converters::meta_iceberg_to_databend;

/// directory containing metadata files
const META_DIR: &str = "metadata";
/// file marking the current version of metadata file
const META_PTR: &str = "metadata/version_hint.text";

/// accessor wrapper as a table
#[allow(unused)]
pub struct IcebergTable {
    /// database that belongs to
    database: String,
    /// name of the current table
    name: String,
    /// root of the table
    tbl_root: Operator,
    /// table metadata
    manifests: TableMetadataV2,
    /// table information
    info: TableInfo,
}

impl IcebergTable {
    /// create a new table on the table directory
    pub async fn try_create_table_from_read(
        catalog: &str,
        tenant: &str,
        database: &str,
        table_name: &str,
        tbl_root: Operator,
    ) -> Result<IcebergTable> {
        // find version_hint.txt, version number can be get from it.
        let hint = tbl_root.object(META_PTR);
        let version: u64 = String::from_utf8(hint.read().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!("invalid version_hint.text: {:?}", e))
        })?)
        .map_err(|e| ErrorCode::ReadTableDataError(format!("invalid version_hint.text: {:?}", e)))?
        .trim()
        .parse()
        .map_err(|e| {
            ErrorCode::ReadTableDataError(format!("invalid version_hint.text: {:?}", e))
        })?;

        // get table metadata from metadata file
        // should be in `metadata/v{version}.metadata.json`, stored as json
        let meta_file_latest = tbl_root.object(&format!("{}/v{}.metadata.json", META_DIR, version));
        let meta_json = meta_file_latest.read().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!(
                "invalid metadata in {}: {:?}",
                meta_file_latest.name(),
                e
            ))
        })?;
        let metadata: TableMetadataV2 =
            serde_json::de::from_slice(meta_json.as_slice()).map_err(|e| {
                ErrorCode::ReadTableDataError(format!(
                    "invalid metadata in {}: {:?}",
                    meta_file_latest.name(),
                    e
                ))
            })?;

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("IcebergTable: '{}'.'{}'", database, table_name),
            name: table_name.to_string(),
            meta: meta_iceberg_to_databend(catalog, &metadata),
            tenant: tenant.to_string(),
            ..Default::default()
        };

        // finish making table
        Ok(Self {
            database: database.to_string(),
            name: table_name.to_string(),
            tbl_root,
            manifests: metadata,
            info,
        })
    }
}

#[async_trait]
impl Table for IcebergTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        todo!()
    }
}

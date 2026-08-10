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

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::PaimonCatalogOption;
use databend_common_meta_app::schema::catalog_id_ident::CatalogId;
use databend_common_meta_app::schema::catalog_name_ident::CatalogNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_paimon::PaimonCatalog;
use databend_common_storages_paimon::PaimonPartInfo;
use databend_common_storages_paimon::PaimonTable;
use futures::StreamExt;
use paimon::Catalog;
use paimon::CatalogOptions;
use paimon::FileSystemCatalog;
use paimon::Options;
use paimon::catalog::Identifier;
use paimon::spec::DataType;
use paimon::spec::IntType;
use paimon::spec::Schema;
use paimon::spec::VarCharType;
use paimon::table::BranchManager;
use tempfile::TempDir;

pub struct TestWarehouse {
    pub _dir: TempDir,
    pub warehouse: String,
}

impl TestWarehouse {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let warehouse = dir.path().to_str().expect("utf8 path").to_string();
        Self {
            _dir: dir,
            warehouse,
        }
    }
}

pub fn catalog_info(warehouse: &str) -> Arc<CatalogInfo> {
    Arc::new(CatalogInfo::new(
        CatalogNameIdent::new(Tenant::new_literal("test"), "paimon_test"),
        CatalogId::new(1),
        CatalogMeta {
            catalog_option: CatalogOption::Paimon(PaimonCatalogOption {
                options: HashMap::from([
                    ("metastore".to_string(), "filesystem".to_string()),
                    ("warehouse".to_string(), warehouse.to_string()),
                ]),
            }),
            created_on: Utc::now(),
        },
    ))
}

pub fn catalog_info_with_secret(warehouse: &str, secret: &str) -> Arc<CatalogInfo> {
    Arc::new(CatalogInfo::new(
        CatalogNameIdent::new(Tenant::new_literal("test"), "paimon_secret"),
        CatalogId::new(2),
        CatalogMeta {
            catalog_option: CatalogOption::Paimon(PaimonCatalogOption {
                options: HashMap::from([
                    ("metastore".to_string(), "filesystem".to_string()),
                    ("warehouse".to_string(), warehouse.to_string()),
                    ("token".to_string(), secret.to_string()),
                    ("password".to_string(), secret.to_string()),
                    ("s3.secret-access-key".to_string(), secret.to_string()),
                ]),
            }),
            created_on: Utc::now(),
        },
    ))
}

pub fn filesystem_catalog(warehouse: &str) -> FileSystemCatalog {
    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "filesystem");
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    FileSystemCatalog::new(options).expect("filesystem catalog")
}

pub async fn setup_append_table(warehouse: &str) -> Identifier {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .expect("create db");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "append_t");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create append table");
    let table = catalog.get_table(&identifier).await.expect("get table");
    write_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"]).await;
    identifier
}

pub async fn setup_metadata_table(warehouse: &str) -> Identifier {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .expect("create db");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .option("test.option", "metadata-value")
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "metadata_t");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create metadata table");
    let table = catalog.get_table(&identifier).await.expect("get table");
    write_batch(&table, vec![1], vec!["a"]).await;
    write_batch(&table, vec![2], vec!["b"]).await;

    let branch_manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    branch_manager
        .create_branch("dev")
        .await
        .expect("create branch");
    let snapshot_manager =
        paimon::SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .expect("get latest snapshot")
        .expect("latest snapshot");
    let tag_manager =
        paimon::TagManager::new(table.file_io().clone(), table.location().to_string());
    tag_manager
        .create("release", &snapshot)
        .await
        .expect("create tag");
    identifier
}

pub async fn setup_multi_split_append_table(warehouse: &str) -> Identifier {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .expect("create db");
    let schema = Schema::builder()
        .column("part", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["part"])
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "multi_split_t");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create partitioned append table");
    let table = catalog.get_table(&identifier).await.expect("get table");
    for part in 0..4 {
        write_partitioned_batch(&table, part, vec![part], vec![format!("p{part}")]).await;
    }
    identifier
}

pub async fn setup_pk_table(warehouse: &str) -> Identifier {
    let catalog = filesystem_catalog(warehouse);
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .expect("create db");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "pk_t");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create pk table");
    let table = catalog.get_table(&identifier).await.expect("get table");
    write_batch(&table, vec![1], vec!["old"]).await;
    write_batch(&table, vec![1], vec!["new"]).await;
    identifier
}

async fn write_batch(table: &paimon::Table, ids: Vec<i32>, names: Vec<&str>) {
    let write_builder = table.new_write_builder();
    let mut table_write = write_builder.new_write().expect("table write");
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(ids)),
        Arc::new(StringArray::from(names)),
    ])
    .expect("record batch");
    table_write
        .write_arrow_batch(&batch)
        .await
        .expect("write batch");
    let messages = table_write.prepare_commit().await.expect("prepare commit");
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .expect("commit");
}

async fn write_partitioned_batch(
    table: &paimon::Table,
    part: i32,
    ids: Vec<i32>,
    names: Vec<String>,
) {
    let write_builder = table.new_write_builder();
    let mut table_write = write_builder.new_write().expect("table write");
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("part", ArrowDataType::Int32, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
    ]));
    let parts = vec![part; ids.len()];
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(parts)),
        Arc::new(Int32Array::from(ids)),
        Arc::new(StringArray::from(names)),
    ])
    .expect("record batch");
    table_write
        .write_arrow_batch(&batch)
        .await
        .expect("write batch");
    let messages = table_write.prepare_commit().await.expect("prepare commit");
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .expect("commit");
}

pub async fn databend_table(warehouse: &str, identifier: &Identifier) -> Arc<dyn Table> {
    databend_table_with_catalog(warehouse, catalog_info(warehouse), identifier).await
}

pub async fn databend_table_with_catalog(
    warehouse: &str,
    catalog_info: Arc<CatalogInfo>,
    identifier: &Identifier,
) -> Arc<dyn Table> {
    let catalog = filesystem_catalog(warehouse);
    let paimon_table = catalog
        .get_table(identifier)
        .await
        .expect("get paimon table");
    let options = match &catalog_info.meta.catalog_option {
        CatalogOption::Paimon(option) => option.options.clone(),
        _ => panic!("expected paimon catalog"),
    };
    PaimonTable::from_paimon_table(catalog_info, options, paimon_table).expect("databend table")
}

pub async fn read_rows_via_paimon(warehouse: &str, identifier: &Identifier) -> Vec<(i32, String)> {
    let catalog = filesystem_catalog(warehouse);
    let table = catalog.get_table(identifier).await.expect("get table");
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.expect("scan plan");
    let table_read = read_builder.new_read().expect("table read");
    let mut rows = Vec::new();
    for split in plan.splits() {
        let mut stream = table_read
            .to_arrow(std::slice::from_ref(split))
            .expect("arrow stream");
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch");
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .or_else(|| batch.column(0).as_any().downcast_ref::<Int32Array>())
                .expect("id column");
            let name_idx = if batch.num_columns() == 3 { 2 } else { 1 };
            let names = batch
                .column(name_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column");
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), names.value(i).to_string()));
            }
        }
    }
    rows
}

pub async fn paimon_catalog(warehouse: &str) -> PaimonCatalog {
    let info = catalog_info(warehouse);
    databend_common_base::runtime::spawn_blocking(move || PaimonCatalog::try_create(info))
        .await
        .expect("join catalog task")
        .expect("paimon catalog")
}

pub fn part_infos(parts: &databend_common_catalog::plan::Partitions) -> Vec<&PaimonPartInfo> {
    parts
        .partitions
        .iter()
        .map(|part| {
            part.as_any()
                .downcast_ref::<PaimonPartInfo>()
                .expect("paimon part")
        })
        .collect()
}

pub async fn scan_splits(warehouse: &str, identifier: &Identifier) -> Vec<paimon::DataSplit> {
    let catalog = filesystem_catalog(warehouse);
    let table = catalog.get_table(identifier).await.expect("get table");
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.expect("scan plan");
    plan.splits().to_vec()
}

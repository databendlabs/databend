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
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::MutexGuard;

use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use chrono::Utc;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::PaimonCatalogOption;
use databend_common_meta_app::schema::catalog_id_ident::CatalogId;
use databend_common_meta_app::schema::catalog_name_ident::CatalogNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_paimon::PaimonTable;
use databend_query::test_kits::TestFixture;
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

static PAIMON_IT_LOCK: Mutex<()> = Mutex::new(());

pub fn paimon_it_lock() -> MutexGuard<'static, ()> {
    PAIMON_IT_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

// 每个测试运行在各自的线程上，而 GlobalInstance 在单测模式下按线程名注册，
// 因此 fixture 必须按线程名各自持有：同一测试内复用，跨测试互不影响，
// 保证每个测试都在自己的线程名下完成 TestFixture::setup 的 GlobalInstance 注册。
static SHARED_TEST_FIXTURES: LazyLock<Mutex<HashMap<String, Arc<TestFixture>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn shared_fixture() -> databend_common_exception::Result<Arc<TestFixture>> {
    let thread_name = std::thread::current()
        .name()
        .unwrap_or("paimon_it")
        .to_string();
    if let Some(fixture) = SHARED_TEST_FIXTURES
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .get(&thread_name)
    {
        return Ok(fixture.clone());
    }
    let fixture = Arc::new(TestFixture::setup().await?);
    Ok(SHARED_TEST_FIXTURES
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .entry(thread_name)
        .or_insert(fixture)
        .clone())
}

pub async fn setup_paimon_fixture() -> databend_common_exception::Result<Arc<TestFixture>> {
    shared_fixture().await
}

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

pub async fn setup_branch_table(warehouse: &str) -> Identifier {
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
    let identifier = Identifier::new("db", "branch_t");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create branch table");
    let table = catalog.get_table(&identifier).await.expect("get table");
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch("dev")
        .await
        .expect("create branch");
    let branch_table = paimon::Table::new(
        table.file_io().clone(),
        Identifier::new("db", "branch_t$branch_dev"),
        format!("{}/branch/branch-dev", table.location()),
        table.schema().clone(),
        None,
    );
    write_partitioned_batch(&branch_table, 0, vec![1], vec!["branch".to_string()]).await;
    write_partitioned_batch(&table, 0, vec![1], vec!["main-0".to_string()]).await;
    write_partitioned_batch(&table, 1, vec![2], vec!["main-1".to_string()]).await;
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

pub async fn databend_table(
    warehouse: &str,
    identifier: &Identifier,
) -> Arc<dyn databend_common_catalog::table::Table> {
    let catalog = filesystem_catalog(warehouse);
    let paimon_table = catalog
        .get_table(identifier)
        .await
        .expect("get paimon table");
    PaimonTable::from_paimon_table(
        catalog_info(warehouse),
        HashMap::from([
            ("metastore".to_string(), "filesystem".to_string()),
            ("warehouse".to_string(), warehouse.to_string()),
        ]),
        paimon_table,
    )
    .expect("databend table")
}

pub fn part_infos(
    parts: &databend_common_catalog::plan::Partitions,
) -> Vec<&databend_common_storages_paimon::PaimonPartInfo> {
    parts
        .partitions
        .iter()
        .map(|part| {
            part.as_any()
                .downcast_ref::<databend_common_storages_paimon::PaimonPartInfo>()
                .expect("paimon part")
        })
        .collect()
}

pub fn create_paimon_catalog_sql(warehouse: &str) -> String {
    format!(
        "CREATE CATALOG paimon_it TYPE = PAIMON CONNECTION = (METASTORE = 'filesystem', WAREHOUSE = '{warehouse}')"
    )
}

pub fn drop_paimon_catalog_sql() -> &'static str {
    "DROP CATALOG IF EXISTS paimon_it"
}

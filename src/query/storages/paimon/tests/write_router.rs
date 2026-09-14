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

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use common::TestWarehouse;
use common::filesystem_catalog;
use databend_common_storages_paimon::PaimonWriteRouter;
use databend_common_storages_paimon::encode_route_key;
use paimon::Catalog;
use paimon::catalog::Identifier;
use paimon::spec::DataType;
use paimon::spec::IntType;
use paimon::spec::Schema;
use paimon::spec::VarCharType;

#[test]
fn test_encode_route_key_keeps_partition_boundary() {
    assert_ne!(
        encode_route_key(b"ab", 1),
        encode_route_key(b"a", 0x62000001),
    );
    assert_eq!(
        encode_route_key(b"part=1", 3),
        encode_route_key(b"part=1", 3)
    );
}

#[tokio::test]
async fn test_router_matches_paimon_fixed_bucket_write() {
    let wh = TestWarehouse::new();
    let catalog = filesystem_catalog(&wh.warehouse);
    catalog
        .create_database("db", false, Default::default())
        .await
        .expect("create db");

    let schema = Schema::builder()
        .column("part", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["part"])
        .primary_key(["part", "id"])
        .option("bucket", "4")
        .build()
        .expect("schema");
    let identifier = Identifier::new("db", "pk_fixed");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create table");
    let table = catalog.get_table(&identifier).await.expect("get table");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("part", ArrowDataType::Int32, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![1, 1, 2])),
        Arc::new(Int32Array::from(vec![10, 11, 20])),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    ])
    .expect("record batch");

    let router = PaimonWriteRouter::try_create(table.schema()).expect("router");
    let routes = router.route_batch(&batch).expect("route batch");
    assert_eq!(routes.len(), batch.num_rows());
    assert_eq!(routes[0].partition, routes[1].partition);
    assert_ne!(routes[0].partition, routes[2].partition);
    assert!(routes.iter().all(|route| (0..4).contains(&route.bucket)));

    let write_builder = table.new_write_builder();
    let mut table_write = write_builder.new_write().expect("table write");
    table_write
        .write_arrow_batch(&batch)
        .await
        .expect("write batch");
    let messages = table_write.prepare_commit().await.expect("prepare commit");

    let routed: HashSet<_> = routes
        .iter()
        .map(|r| (r.partition.clone(), r.bucket))
        .collect();
    let committed: HashSet<_> = messages
        .iter()
        .map(|m| (m.partition.clone(), m.bucket))
        .collect();
    assert_eq!(routed, committed);
}

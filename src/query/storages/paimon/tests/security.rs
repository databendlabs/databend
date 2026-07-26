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

use common::TestWarehouse;
use common::catalog_info_with_secret;
use common::databend_table_with_catalog;
use common::filesystem_catalog;
use common::setup_append_table;
use databend_common_catalog::table::Table;
use databend_common_storages_paimon::PaimonTable;
use databend_common_storages_paimon::PaimonTableDescriptor;
use paimon::Catalog;
use paimon::catalog::Identifier;

const FAKE_SECRET: &str = "paimon-test-secret-do-not-leak";

#[tokio::test]
async fn test_engine_options_and_debug_do_not_leak_catalog_secrets() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let catalog_info = catalog_info_with_secret(&wh.warehouse, FAKE_SECRET);
    let table = databend_table_with_catalog(&wh.warehouse, catalog_info, &identifier).await;
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table");

    let engine_options_json = paimon_table.engine_options_json().expect("engine options");
    assert!(
        !engine_options_json.contains(FAKE_SECRET),
        "engine_options must not embed catalog secrets"
    );
    assert!(
        !engine_options_json.contains("s3.secret-access-key"),
        "engine_options must not embed secret option keys"
    );

    let descriptor: PaimonTableDescriptor =
        serde_json::from_str(&engine_options_json).expect("descriptor json");
    let debug = format!("{descriptor:?}");
    assert!(!debug.contains(FAKE_SECRET));

    let table_info = paimon_table.get_table_info();
    let show_create = format!("{:?}", table_info.meta.engine_options);
    assert!(!show_create.contains(FAKE_SECRET));
}

#[test]
fn test_worker_can_rebuild_catalog_options_from_table_info() {
    let wh = TestWarehouse::new();
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let identifier = rt.block_on(setup_append_table(&wh.warehouse));
    let catalog_info = catalog_info_with_secret(&wh.warehouse, FAKE_SECRET);
    let table = rt.block_on(databend_table_with_catalog(
        &wh.warehouse,
        catalog_info.clone(),
        &identifier,
    ));
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table");
    let rebuilt = PaimonTable::try_create(paimon_table.get_table_info().clone()).expect("rebuild");
    let rebuilt = rebuilt
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon");
    assert_eq!(
        rebuilt.get_table_info().catalog_info.meta.catalog_option,
        catalog_info.meta.catalog_option
    );
    rt.block_on(async {
        let inner = filesystem_catalog(&wh.warehouse);
        let loaded = inner
            .get_table(&Identifier::new("db", "append_t"))
            .await
            .expect("load");
        assert_eq!(loaded.identifier().database(), "db");
    });
}

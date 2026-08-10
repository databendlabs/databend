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

//! Offline tests for CONNECTION option key quoting.
//!
//! SQL parser stores dotted keys with quotes intact (e.g. `"s3.region"`).
//! Catalog open must strip them before paimon FileIO. Local warehouse only —
//! no MinIO/network required. Worker rebuild strip is covered by unit tests
//! in `table.rs`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::TestWarehouse;
use common::databend_table_with_catalog;
use common::setup_append_table;
use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::CatalogIdIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::PaimonCatalogOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_paimon::PaimonCatalog;
use databend_common_storages_paimon::PaimonTable;

fn catalog_info_with_options(name: &str, options: HashMap<String, String>) -> Arc<CatalogInfo> {
    Arc::new(CatalogInfo {
        id: CatalogIdIdent::new(Tenant::new_literal("dummy"), 0).into(),
        name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), name).into(),
        meta: CatalogMeta {
            catalog_option: CatalogOption::Paimon(PaimonCatalogOption { options }),
            created_on: chrono::Utc::now(),
        },
    })
}

/// Local warehouse options as the SQL parser would store dotted keys (quotes kept).
fn quoted_local_options(warehouse: &str) -> HashMap<String, String> {
    HashMap::from([
        ("metastore".to_string(), "filesystem".to_string()),
        ("warehouse".to_string(), warehouse.to_string()),
        (
            "\"s3.endpoint\"".to_string(),
            "http://127.0.0.1:9900".to_string(),
        ),
        ("\"s3.access-key\"".to_string(), "minioadmin".to_string()),
        ("\"s3.secret-key\"".to_string(), "minioadmin".to_string()),
        ("\"s3.region\"".to_string(), "us-east-1".to_string()),
        ("\"s3.path-style-access\"".to_string(), "true".to_string()),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn try_create_strips_quoted_option_keys() {
    let wh = TestWarehouse::new();
    let info = catalog_info_with_options("paimon_quoted", quoted_local_options(&wh.warehouse));
    let cat = PaimonCatalog::try_create(info).expect("try_create with quoted keys");
    let opts = cat.catalog_options();

    assert_eq!(opts.get("s3.region").map(String::as_str), Some("us-east-1"));
    assert_eq!(
        opts.get("s3.endpoint").map(String::as_str),
        Some("http://127.0.0.1:9900")
    );
    assert_eq!(
        opts.get("warehouse").map(String::as_str),
        Some(wh.warehouse.as_str())
    );
    assert!(!opts.contains_key("\"s3.region\""));
    assert!(!opts.contains_key("\"s3.endpoint\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn try_create_accepts_unquoted_option_keys() {
    let wh = TestWarehouse::new();
    let options = HashMap::from([
        ("metastore".to_string(), "filesystem".to_string()),
        ("warehouse".to_string(), wh.warehouse.clone()),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]);
    let info = catalog_info_with_options("paimon_plain", options);
    let cat = PaimonCatalog::try_create(info).expect("try_create with plain keys");
    assert_eq!(
        cat.catalog_options().get("s3.region").map(String::as_str),
        Some("us-east-1")
    );
}

/// Worker rebuild: meta keeps quoted keys; try_create must succeed (uses strip).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn worker_rebuild_from_table_info_with_quoted_keys() {
    let wh = TestWarehouse::new();
    let identifier = setup_append_table(&wh.warehouse).await;
    let catalog_info =
        catalog_info_with_options("paimon_worker", quoted_local_options(&wh.warehouse));
    let table = databend_table_with_catalog(&wh.warehouse, catalog_info, &identifier).await;
    let paimon_table = table
        .as_any()
        .downcast_ref::<PaimonTable>()
        .expect("paimon table");

    let CatalogOption::Paimon(raw) = &paimon_table
        .get_table_info()
        .catalog_info
        .meta
        .catalog_option
    else {
        panic!("expected paimon catalog option");
    };
    assert!(
        raw.options.contains_key("\"s3.region\""),
        "meta should retain quoted keys for the rebuild regression"
    );

    let rebuilt =
        PaimonTable::try_create(paimon_table.get_table_info().clone()).expect("worker rebuild");
    assert_eq!(rebuilt.name(), paimon_table.name());
}

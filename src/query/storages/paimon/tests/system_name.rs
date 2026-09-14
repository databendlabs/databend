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

use common::TestWarehouse;
use common::paimon_catalog;
use common::setup_append_table;
use databend_common_catalog::catalog::Catalog;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_paimon::PaimonSystemTableKind;
use databend_common_storages_paimon::ParsedName;
use databend_common_storages_paimon::parse_system_name;

#[test]
fn test_parse_system_table_name() {
    assert_eq!(
        parse_system_name("orders"),
        ParsedName::Base("orders".to_string())
    );
    assert_eq!(parse_system_name("orders$Snapshots"), ParsedName::System {
        base: "orders".to_string(),
        branch: None,
        kind: PaimonSystemTableKind::Snapshots,
    });
    assert_eq!(
        parse_system_name("orders$branch_dev$files"),
        ParsedName::System {
            base: "orders".to_string(),
            branch: Some("dev".to_string()),
            kind: PaimonSystemTableKind::Files,
        }
    );
    assert!(matches!(
        parse_system_name("orders$unknown"),
        ParsedName::UnknownSystem { base, suffix }
            if base == "orders" && suffix == "unknown"
    ));
    assert_eq!(
        parse_system_name("orders$dev$files"),
        ParsedName::Base("orders$dev$files".to_string())
    );
}

#[test]
fn test_system_table_schemas_are_non_empty_and_unique() {
    assert_eq!(PaimonSystemTableKind::ALL.len(), 11);
    for kind in PaimonSystemTableKind::ALL {
        let schema = kind.schema();
        assert!(schema.num_fields() > 0, "{kind:?} schema must not be empty");
        let unique_names = schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<HashSet<_>>();
        assert_eq!(
            unique_names.len(),
            schema.num_fields(),
            "{kind:?} schema field names must be unique"
        );
    }
}

#[tokio::test]
async fn test_database_resolves_system_table_names() {
    let wh = TestWarehouse::new();
    let _ = setup_append_table(&wh.warehouse).await;
    let catalog = paimon_catalog(&wh.warehouse).await;
    let tenant = Tenant::new_literal("test");
    let database = catalog.get_database(&tenant, "db").await.expect("database");

    let table = database
        .get_table("append_t$snapshots")
        .await
        .expect("snapshots system table");
    assert_eq!(table.name(), "append_t$snapshots");
    assert_eq!(table.schema(), PaimonSystemTableKind::Snapshots.schema());

    let err = match database.get_table("append_t$unknown").await {
        Ok(_) => panic!("unknown system table must be rejected"),
        Err(err) => err,
    };
    assert!(err.message().contains("append_t$unknown"));
}

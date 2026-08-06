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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_storages_common_session::TempTblMgr;
use databend_storages_common_session::abort_staged_temp_table;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use parking_lot::Mutex;

fn create_table_req(
    table_name: &str,
    engine: &str,
    create_option: CreateOption,
    as_dropped: bool,
) -> CreateTableReq {
    let mut table_meta = TableMeta {
        engine: engine.to_string(),
        ..Default::default()
    };
    table_meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_string(), "1".to_string());

    CreateTableReq {
        create_option,
        catalog_name: None,
        name_ident: TableNameIdent {
            tenant: Tenant::new_literal("tenant"),
            db_name: "db".to_string(),
            table_name: table_name.to_string(),
        },
        table_meta,
        as_dropped,
        materialized_view: None,
        table_properties: None,
        table_partition: None,
    }
}

#[tokio::test]
async fn test_aborted_ctas_does_not_leave_a_temporary_table() {
    let mut mgr = TempTblMgr::default();
    let staged = mgr
        .create_table(
            create_table_req("t", "MEMORY", CreateOption::Create, true),
            "session".to_string(),
        )
        .unwrap();
    let mgr = Arc::new(Mutex::new(mgr));

    {
        let guard = mgr.lock();
        assert!(!guard.is_temp_table("db", "t"));
        assert!(guard.list_tables().unwrap().is_empty());
        assert!(!guard.is_empty());
    }

    abort_staged_temp_table(mgr.clone(), staged.table_id, "session")
        .await
        .unwrap();
    // Cleanup is idempotent.
    abort_staged_temp_table(mgr.clone(), staged.table_id, "session")
        .await
        .unwrap();
    assert!(mgr.lock().is_empty());
}

#[tokio::test]
async fn test_abort_preserves_staged_table_when_cleanup_fails() {
    let mut mgr = TempTblMgr::default();
    let staged = mgr
        .create_table(
            create_table_req("t", "FUSE", CreateOption::Create, true),
            "session".to_string(),
        )
        .unwrap();
    mgr.staged_tables
        .get_mut(&staged.table_id)
        .unwrap()
        .meta
        .options
        .remove(OPT_KEY_DATABASE_ID);
    let mgr = Arc::new(Mutex::new(mgr));

    let err = abort_staged_temp_table(mgr.clone(), staged.table_id, "session")
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::INTERNAL);

    let guard = mgr.lock();
    assert!(guard.staged_tables.contains_key(&staged.table_id));
    assert!(guard.list_tables().unwrap().is_empty());
    assert!(!guard.is_empty());
}

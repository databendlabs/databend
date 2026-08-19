// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use databend_common_catalog::table::Table;
use databend_common_config::MetaConfig;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::send_txn;
use databend_common_meta_api::txn_core_util::txn_replace_exact;
use databend_common_meta_app::schema::MVDefinitionIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::materialized_view::MaterializedViewRefresh;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::TxnRequest;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON;
use futures::TryStreamExt;

async fn new_local_meta() -> MetaStore {
    let meta_config = MetaConfig::default();
    let config = meta_config.to_meta_grpc_client_conf();
    let provider = MetaStoreProvider::new(config);
    provider
        .create_meta_store::<DatabendRuntime>()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_persists_invalid_definition_reason() -> anyhow::Result<()> {
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

    let mut ee_setup = EESetup::new();
    ee_setup.config_mut().meta.endpoints = endpoints;
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    let db_name = "test_mv_invalid_reason";
    let source_name = "source";
    let mv_name = "mv";
    fixture
        .execute_command(&format!("create database {db_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "create table {db_name}.{source_name} change_tracking = true as \
             select number from numbers(3)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "create materialized view {db_name}.{mv_name} as \
             select number from {db_name}.{source_name} where 1 = 1"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let catalog = ctx.get_default_catalog()?;
    let mv_table = catalog.get_table(&tenant, db_name, mv_name).await?;
    let stale_mv_table = FuseTable::try_from_table(mv_table.as_ref())?;
    let stale_mv_seq = stale_mv_table.get_table_info().ident.seq;
    let definition_ident = MVDefinitionIdent::new(&tenant, mv_table.get_id());
    let seqv = meta
        .get_pb(&definition_ident)
        .await?
        .expect("materialized view definition must exist");
    let mut definition = seqv.data;
    // Valid SQL that is not a query. Physical parse happens before the
    // checkpoint short-circuit, so refresh always records this failure.
    definition.query = "CREATE TABLE t(a INT)".to_string();

    let mut txn = TxnRequest::default();
    txn_replace_exact(&mut txn, &definition_ident, seqv.seq, &definition)?;
    let (success, _) = send_txn(&meta, txn).await?;
    assert!(
        success,
        "corrupting the persisted MV definition must succeed"
    );

    // Advance TableMeta after retaining the old FuseTable. Persisting the parse failure must
    // merge over this newer version instead of losing the marker to an Exact-seq mismatch.
    catalog
        .upsert_table_option(&tenant, db_name, UpsertTableOptionReq {
            table_id: stale_mv_table.get_id(),
            seq: MatchSeq::Exact(stale_mv_seq),
            options: HashMap::from([(
                OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(),
                Some("superseded reason".to_string()),
            )]),
        })
        .await?;

    let catalog_name = ctx.get_current_catalog();
    let err = MaterializedViewRefresh::create(
        stale_mv_table,
        ctx.clone(),
        &catalog_name,
        db_name,
        mv_name,
    )
    .await
    .err()
    .expect("refresh must reject an unparsable persisted physical definition");
    assert_eq!(err.code(), ErrorCode::INVALID_MATERIALIZED_VIEW);
    assert!(
        err.message()
            .contains("invalid materialized view physical query"),
        "unexpected refresh error: {}",
        err.message()
    );

    let ctx = fixture.new_query_ctx().await?;
    let catalog_name = ctx.get_current_catalog();
    ctx.evict_table_from_cache(&catalog_name, db_name, mv_name)?;
    let mv_table = ctx.get_table(&catalog_name, db_name, mv_name).await?;
    assert_eq!(
        mv_table
            .options()
            .get(OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON)
            .map(String::as_str),
        Some("invalid materialized view physical query")
    );

    let blocks: Vec<DataBlock> = fixture
        .execute_query(&format!(
            "select invalid, invalid_reason from system.materialized_views \
             where database = '{db_name}' and name = '{mv_name}'"
        ))
        .await?
        .try_collect()
        .await?;
    assert_eq!(blocks.len(), 1);
    let block = &blocks[0];
    assert_eq!(block.num_rows(), 1);
    assert_eq!(
        block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::Boolean(true)
    );
    assert_eq!(
        block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::String("invalid materialized view physical query")
    );

    Ok(())
}

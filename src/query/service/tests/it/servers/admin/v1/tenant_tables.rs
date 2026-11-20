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

use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_storages_fuse::FuseTable;
use databend_meta_client::types::MatchSeq;
use databend_query::servers::admin::AdminService;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::*;
use http::Method;
use http::StatusCode;
use http::Uri;
use http::header;
use maplit::btreemap;
use poem::Endpoint;
use poem::Request;
use pretty_assertions::assert_eq;
use serde_json::Value;

/// A newer release may attach a `BLOOM` table index to table metadata that this
/// release still has to list. The admin tables API must keep returning such a
/// table together with its `SHOW CREATE TABLE` text instead of dropping it.
#[tokio::test(flavor = "multi_thread")]
async fn test_list_tenant_tables_with_bloom_index_meta() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = ctx
        .get_table(
            "default",
            &fixture.default_db_name(),
            &fixture.default_table_name(),
        )
        .await?;

    // Simulate metadata written by a release that supports BLOOM table indexes.
    let table_info = table.get_table_info().clone();
    let mut meta = table_info.meta.clone();
    let column_id = meta.schema.field_with_name("id")?.column_id();
    meta.indexes.insert("id_bloom".to_string(), TableIndex {
        index_type: TableIndexType::Bloom,
        name: "id_bloom".to_string(),
        column_ids: vec![column_id],
        sync_creation: true,
        version: "0".to_string(),
        options: btreemap! {"filter_type".to_string() => "binary_fuse32".to_string()},
    });
    catalog
        .update_single_table_meta(
            &fixture.default_tenant(),
            UpdateTableMetaReq {
                table_id: table_info.ident.table_id,
                seq: MatchSeq::Exact(table_info.ident.seq),
                new_table_meta: meta,
                base_snapshot_location: FuseTable::try_from_table(table.as_ref())?.snapshot_loc(),
                lvt_check: None,
            },
            &table_info,
        )
        .await?;

    let srv = AdminService::create(&ConfigBuilder::create().with_management_mode().build());
    let uri = format!(
        "/v1/tenants/{}/tables",
        fixture.default_tenant().tenant_name()
    );
    let response = srv
        .build_router()
        .get_response(
            Request::builder()
                .uri(Uri::from_maybe_shared(uri)?)
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::GET)
                .finish(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_vec().await?;
    let resp = serde_json::from_slice::<Value>(&body)?;
    assert_eq!(resp["warnings"], Value::Array(vec![]));

    let tables = resp["tables"].as_array().expect("tables array");
    let listed = tables
        .iter()
        .find(|t| {
            t["database"] == fixture.default_db_name().as_str()
                && t["table"] == fixture.default_table_name().as_str()
        })
        .expect("table with BLOOM index meta must be listed");
    assert_eq!(listed["engine"], "FUSE");
    let create_query = listed["create_query"].as_str().expect("create_query");
    assert!(
        create_query.contains("SYNC BLOOM INDEX id_bloom (id) filter_type = 'binary_fuse32'"),
        "unexpected create_query: {create_query}"
    );

    Ok(())
}

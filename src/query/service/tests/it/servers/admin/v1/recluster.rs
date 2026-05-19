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

use databend_query::servers::admin::v1::clustering_information::clustering_information_handler;
use databend_query::test_kits::*;
use http::Method;
use http::StatusCode;
use http::Uri;
use http::header;
use poem::Endpoint;
use poem::Request;
use poem::Route;
use poem::get;
use pretty_assertions::assert_eq;
use serde_json::Value;

#[tokio::test(flavor = "multi_thread")]
async fn test_clustering_information() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let recluster_router = Route::new().at(
        "/v1/tenants/:tenant/databases/:database/tables/:table/clustering_information",
        get(clustering_information_handler),
    );

    let uri = format!(
        "/v1/tenants/{}/databases/{}/tables/{}/clustering_information",
        fixture.default_tenant().tenant_name(),
        fixture.default_db_name(),
        fixture.default_table_name(),
    );
    let response = recluster_router
        .call(
            Request::builder()
                .uri(Uri::from_maybe_shared(uri)?)
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_vec().await.unwrap();
    let resp = serde_json::from_slice::<Value>(&body)?;
    assert_eq!(resp["cluster_key"], "(id)");
    assert_eq!(resp["type"], "linear");
    assert!(resp["timestamp"].as_i64().is_some());
    assert_eq!(resp["info"]["total_block_count"].as_u64(), Some(0));
    assert_eq!(resp["info"]["constant_block_count"].as_u64(), Some(0));

    Ok(())
}

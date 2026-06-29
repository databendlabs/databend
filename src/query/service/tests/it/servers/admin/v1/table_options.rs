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

use databend_query::servers::admin::AdminService;
use databend_query::test_kits::*;
use http::Method;
use http::StatusCode;
use http::Uri;
use http::header;
use poem::Endpoint;
use poem::Request;
use pretty_assertions::assert_eq;
use serde_json::Value;

#[tokio::test(flavor = "multi_thread")]
async fn test_set_and_unset_table_options() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let srv = AdminService::create(&ConfigBuilder::create().with_management_mode().build());

    let uri = format!(
        "/v1/tenants/{}/databases/{}/tables/{}/options",
        fixture.default_tenant().tenant_name(),
        fixture.default_db_name(),
        fixture.default_table_name(),
    );

    // Set a table option.
    let response = srv
        .build_router()
        .get_response(
            Request::builder()
                .uri(Uri::from_maybe_shared(uri.clone())?)
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::POST)
                .body(r#"{"block_per_segment":"500"}"#),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_vec().await.unwrap();
    let resp = serde_json::from_slice::<Value>(&body)?;
    assert_eq!(resp["success"], true);

    let table = fixture.latest_default_table().await?;
    assert_eq!(
        table.get_table_info().options().get("block_per_segment"),
        Some(&"500".to_string()),
    );

    // Unset the table option.
    let response = srv
        .build_router()
        .get_response(
            Request::builder()
                .uri(Uri::from_maybe_shared(uri.clone())?)
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::DELETE)
                .body(r#"["block_per_segment"]"#),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_vec().await.unwrap();
    let resp = serde_json::from_slice::<Value>(&body)?;
    assert_eq!(resp["success"], true);

    let table = fixture.latest_default_table().await?;
    assert_eq!(
        table.get_table_info().options().get("block_per_segment"),
        None,
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_invalid_table_option() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let srv = AdminService::create(&ConfigBuilder::create().with_management_mode().build());

    let uri = format!(
        "/v1/tenants/{}/databases/{}/tables/{}/options",
        fixture.default_tenant().tenant_name(),
        fixture.default_db_name(),
        fixture.default_table_name(),
    );

    // storage_format is not allowed to be changed via this API.
    let response = srv
        .build_router()
        .get_response(
            Request::builder()
                .uri(Uri::from_maybe_shared(uri)?)
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::POST)
                .body(r#"{"storage_format":"native"}"#),
        )
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    Ok(())
}

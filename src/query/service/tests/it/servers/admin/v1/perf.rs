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
use poem::Endpoint;
use poem::Request;
use poem::Response;
use pretty_assertions::assert_eq;
use serde_json::Value;

async fn get(srv: &AdminService, uri: &str) -> anyhow::Result<Response> {
    Ok(srv
        .build_router()
        .get_response(
            Request::builder()
                .uri(uri.parse::<Uri>()?)
                .method(Method::GET)
                .finish(),
        )
        .await)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_perf_bad_requests() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let srv = AdminService::create(&ConfigBuilder::create().build());

    for (uri, status) in [
        ("/debug/perf/cpu?seconds=0", StatusCode::BAD_REQUEST),
        ("/debug/perf/cpu?seconds=301", StatusCode::BAD_REQUEST),
        (
            "/debug/perf/cpu?seconds=1&query_id=x",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/debug/perf/memory?seconds=1&max_seconds=1",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/debug/perf/memory?seconds=1&format=xml",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/debug/perf/memory?seconds=1&format=folded",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/debug/perf/memory?query_id=not-running",
            StatusCode::NOT_FOUND,
        ),
        (
            "/debug/perf/cpu?query_id=not-running",
            StatusCode::NOT_FOUND,
        ),
    ] {
        assert_eq!(get(&srv, uri).await?.status(), status, "{uri}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_perf_memory_seconds() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let srv = AdminService::create(&ConfigBuilder::create().build());

    let response = get(&srv, "/debug/perf/memory?seconds=1&format=json").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_vec().await?;
    let resp = serde_json::from_slice::<Value>(&body)?;
    assert_eq!(resp["mode"], "memory");
    assert_eq!(resp["target"], "node");
    assert_eq!(resp["stop_reason"], "seconds");
    assert_eq!(resp["rows"][0]["level"], "summary");

    // Only one profile samples the allocations of the node at a time.
    let first = get(&srv, "/debug/perf/memory?seconds=2&format=json");
    let second = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        get(&srv, "/debug/perf/memory?seconds=1").await
    };
    let (first, second) = futures::future::join(first, second).await;
    assert_eq!(first?.status(), StatusCode::OK);
    assert_eq!(second?.status(), StatusCode::CONFLICT);
    Ok(())
}

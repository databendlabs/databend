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

use databend_query::servers::admin::v1::perf::process_perf_page;
use databend_query::servers::admin::v1::perf::query_perf_page;
use http::Method;
use http::StatusCode;
use http::Uri;
use poem::Endpoint;
use poem::Request;
use poem::Route;
use poem::get;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_perf_page_escapes_query_id() -> anyhow::Result<()> {
    let ep = Route::new().at("/v1/queries/:query_id/perf", get(query_perf_page));

    let response = ep
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/queries/%3Cscript%3Eq1/perf"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_string().await?;
    assert!(body.contains("CPU profile of query &lt;script&gt;q1"));
    assert!(!body.contains("<script>q1"));
    assert!(!body.contains("{{"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_perf_page() -> anyhow::Result<()> {
    let ep = Route::new().at("/v1/perf", get(process_perf_page));

    let response = ep
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/perf"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_string().await?;
    assert!(body.contains("CPU profile of every thread on every node"));
    assert!(body.contains(r#"id="seconds" type="number" value="60" min="1" max="600""#));
    assert!(!body.contains("{{"));
    Ok(())
}

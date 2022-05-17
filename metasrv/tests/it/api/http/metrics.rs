// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use databend_meta::api::http::v1::metrics::metrics_handler;
use databend_meta::meta_service::MetaNode;
use poem::get;
use poem::http::Method;
use poem::http::StatusCode;
use poem::http::Uri;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;

#[tokio::test]
async fn test_metrics() -> common_exception::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = MetaSrvTestContext::new(0);

    let meta_node = MetaNode::start(&tc.config.raft_config).await?;

    let cluster_router = Route::new()
        .at("/v1/metrics", get(metrics_handler))
        .data(meta_node.clone());
    let response = cluster_router
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/metrics"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().into_vec().await.unwrap();
    let metrics =
        serde_json::from_str::<serde_json::Value>(String::from_utf8_lossy(&body).as_ref())?;

    metrics["has_leader"].as_u64().unwrap();

    meta_node.stop().await?;

    Ok(())
}

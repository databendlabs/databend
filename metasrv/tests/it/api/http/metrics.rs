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
use common_tracing::tracing;
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

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_metrics() -> common_exception::Result<()> {
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

    meta_node.stop().await?;

    Ok(())
}

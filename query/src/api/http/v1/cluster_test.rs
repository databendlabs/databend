// Copyright 2020 Datafuse Labs.
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

use axum::body::Body;
use axum::handler::get;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::{self};
use axum::AddExtensionLayer;
use axum::Router;
use common_exception::Result;
use common_management::NodeInfo;
use common_runtime::tokio;
use pretty_assertions::assert_eq;
use tower::ServiceExt;

use crate::api::http::v1::cluster::*;
use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_cluster() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let cluster_router = Router::new()
        .route("/v1/cluster/list", get(cluster_list_handler))
        .layer(AddExtensionLayer::new(sessions));

    // List Node
    {
        let response = cluster_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/cluster/list")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .method(http::Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let nodes =
            serde_json::from_str::<Vec<NodeInfo>>(&String::from_utf8_lossy(&*body.to_vec()))?;
        assert_eq!(nodes.len(), 1);
    }

    Ok(())
}

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

use common_exception::Result;
use common_runtime::tokio;

#[tokio::test]
async fn test_cluster() -> Result<()> {
    use axum::body::Body;
    use axum::handler::get;
    use axum::handler::post;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::http::{self};
    use axum::AddExtensionLayer;
    use axum::Router;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tower::ServiceExt;

    use crate::api::http::v1::cluster::*;
    use crate::clusters::Cluster;
    use crate::configs::Config; // for `app.oneshot()`

    let conf = Config::default();
    let cluster = Cluster::create_global(conf.clone())?;
    let cluster_router = Router::new()
        .route("/v1/cluster/add", post(cluster_add_handler))
        .route("/v1/cluster/list", get(cluster_list_handler))
        .route("/v1/cluster/remove", post(cluster_remove_handler))
        .layer(AddExtensionLayer::new(cluster));
    // Add node
    {
        let response = cluster_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/cluster/add")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .method(http::Method::POST)
                    .body(Body::from(
                        serde_json::to_vec(&json!(&ClusterNodeRequest {
                            name: "9090".to_string(),
                            priority: 8,
                            address: "127.0.0.1:9090".to_string()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&*body.to_vec()), "{\"name\":\"9090\",\"priority\":8,\"address\":\"127.0.0.1:9090\",\"local\":true,\"sequence\":0}");
    }

    // Add another node.
    {
        let response = cluster_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/cluster/add")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .method(http::Method::POST)
                    .body(Body::from(
                        serde_json::to_vec(&json!(&ClusterNodeRequest {
                            name: "9091".to_string(),
                            priority: 9,
                            address: "127.0.0.1:9091".to_string()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&*body.to_vec()), "{\"name\":\"9091\",\"priority\":9,\"address\":\"127.0.0.1:9091\",\"local\":false,\"sequence\":1}");
    }

    // List Node
    {
        let response = cluster_router.clone()
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
        assert_eq!(String::from_utf8_lossy(&*body.to_vec()), "[{\"name\":\"9090\",\"priority\":8,\"address\":\"127.0.0.1:9090\",\"local\":true,\"sequence\":0},{\"name\":\"9091\",\"priority\":9,\"address\":\"127.0.0.1:9091\",\"local\":false,\"sequence\":1}]");
    }

    // Remove.
    {
        let response = cluster_router.clone()
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/cluster/remove")
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .method(http::Method::POST)
                    .body(Body::from(
                        serde_json::to_vec(&json!(&ClusterNodeRequest {
                            name: "9091".to_string(),
                            priority: 9,
                            address: "127.0.0.1:9091".to_string()
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(&*body.to_vec()),
            "removed node \"9091\""
        );
    }

    // Check.
    {
        let response = cluster_router
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
        assert_eq!(String::from_utf8_lossy(&*body.to_vec()), "[{\"name\":\"9090\",\"priority\":8,\"address\":\"127.0.0.1:9090\",\"local\":true,\"sequence\":0}]");
    }


    Ok(())
}

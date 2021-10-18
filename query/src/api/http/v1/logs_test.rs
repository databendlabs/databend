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
use common_base::tokio;
use common_exception::Result;
use pretty_assertions::assert_eq;
use tower::ServiceExt;

use crate::api::http::v1::logs::logs_handler;
use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_logs() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;

    let test_router = Router::new()
        .route("/v1/logs", get(logs_handler))
        .layer(AddExtensionLayer::new(sessions));
    {
        let response = test_router
            .oneshot(
                Request::builder()
                    .uri("/v1/logs")
                    .method(http::Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
    Ok(())
}

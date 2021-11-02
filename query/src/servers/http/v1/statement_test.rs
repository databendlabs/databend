// Copyright 2021 Datafuse Labs.
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
use axum::handler::post;
use axum::http;
use axum::http::Request;
use axum::http::StatusCode;
use axum::AddExtensionLayer;
use axum::Router;
use common_base::tokio;
use common_exception::Result;
use pretty_assertions::assert_eq;
use tower::ServiceExt;

use crate::servers::http::v1::statement::statement_handler;
use crate::servers::http::v1::statement::HttpQueryResult;
use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_statement() -> Result<()> {
    {
        let (status, result) = test_sql("select * from system.tables limit 10", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.unwrap().len(), 10);
        assert!(!result.error.is_some());
    }
    {
        let (status, result) = test_sql("select * from tables limit 10", Some("system")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.unwrap().len(), 10);
        assert!(!result.error.is_some());
    }
    {
        let (status, result) = test_sql("show tables", Some("system")).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(!result.data.unwrap().is_empty());
        assert!(!result.error.is_some());
    }
    {
        let (status, result) = test_sql("show tables", Some("")).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.unwrap().len(), 0);
        assert!(!result.error.is_some());
    }
    {
        let (status, result) = test_sql("bad sql", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data, None);
        assert!(result.error.is_some());
    }
    Ok(())
}

async fn test_sql(
    sql: &'static str,
    database: Option<&str>,
) -> Result<(StatusCode, HttpQueryResult)> {
    let path = "/v1/statement";
    let sessions = SessionManagerBuilder::create().build()?;
    let cluster_router = Router::new()
        .route(path, post(statement_handler))
        .layer(AddExtensionLayer::new(sessions));
    let uri = match database {
        Some(db) => format!("{}?db={:}", path, db),
        None => path.into(),
    };
    let response = cluster_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .method(http::Method::POST)
                .body(Body::from(sql))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let result = serde_json::from_slice::<HttpQueryResult>(&body[..])?;
    Ok((status, result))
}

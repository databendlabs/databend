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

use common_base::tokio;
use common_exception::Result;
use databend_query::servers::http::v1::middleware::HTTPSessionMiddleware;
use databend_query::servers::http::v1::statement_handler;
use databend_query::servers::http::v1::QueryResponse;
use poem::http::Method;
use poem::http::StatusCode;
use poem::post;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_statement() -> Result<()> {
    {
        let (status, result) = test_sql("select * from system.tables limit 10", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "%{:?}", result.error);
        assert_eq!(result.data.len(), 10);
    }
    {
        let (status, result) = test_sql("select * from tables limit 10", Some("system")).await?;
        assert!(result.error.is_none(), "%{:?}", result.error);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.data.len(), 10);
    }
    {
        let (status, result) = test_sql("show tables", Some("system")).await?;
        assert!(result.error.is_none(), "%{:?}", result.error);
        assert_eq!(status, StatusCode::OK);
        assert!(!result.data.is_empty());
    }
    {
        let (status, result) = test_sql("show tables", Some("")).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_none(), "%{:?}", result.error);
        assert!(result.data.is_empty());
    }
    {
        let (status, result) = test_sql("bad sql", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(result.error.is_some());
        assert!(result.data.is_empty());
    }
    Ok(())
}

async fn test_sql(
    sql: &'static str,
    database: Option<&str>,
) -> Result<(StatusCode, QueryResponse)> {
    let path = "/v1/statement";
    let session_manager = SessionManagerBuilder::create().build()?;
    let cluster_router = Route::new()
        .at(path, post(statement_handler))
        .with(HTTPSessionMiddleware { session_manager });
    let uri = match database {
        Some(db) => format!("{}?db={:}", path, db),
        None => path.into(),
    };
    let response = cluster_router
        .call(
            Request::builder()
                .uri(uri.parse().unwrap())
                .method(Method::POST)
                .body(sql),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().into_vec().await.unwrap();
    let result = serde_json::from_slice::<QueryResponse>(&body)?;
    Ok((status, result))
}

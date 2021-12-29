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
use common_meta_types::NodeInfo;
use databend_query::api::http::v1::cluster::*;
use poem::get;
use poem::http::header;
use poem::http::Method;
use poem::http::StatusCode;
use poem::http::Uri;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

use crate::tests::SessionManagerBuilder;

#[tokio::test]
async fn test_cluster() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let cluster_router = Route::new()
        .at("/v1/cluster/list", get(cluster_list_handler))
        .data(sessions);

    // List Node
    {
        let response = cluster_router
            .call(
                Request::builder()
                    .uri(Uri::from_static("/v1/cluster/list"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .method(Method::GET)
                    .finish(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().into_vec().await.unwrap();
        let nodes = serde_json::from_str::<Vec<NodeInfo>>(&String::from_utf8_lossy(&body))?;
        assert_eq!(nodes.len(), 1);
    }

    Ok(())
}

// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::tokio;
use databend_meta::api::http::v1::config::config_handler;
use databend_meta::configs::Config;
use http::Method;
use http::StatusCode;
use http::Uri;
use poem::get;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread")]
async fn test_config() -> anyhow::Result<()> {
    let conf = Config::default();
    let cluster_router = Route::new()
        .at("/v1/config", get(config_handler))
        .data(conf.clone());

    let response = cluster_router
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/config"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

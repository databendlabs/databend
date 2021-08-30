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

// use std::convert::Infallible;
//
// use axum::body::HttpBody;
// use axum::handler::get;
// use axum::http::Request;
// use axum::AddExtensionLayer;
// use axum::BoxError;
// use axum::Json;
// use axum::Router;
// use common_exception::Result;
// use hyper::body::Bytes;
// use hyper::service::Service;
// use tonic::body::Body;
//
// use crate::clusters::ClusterRef;
// use crate::configs::Config;
//
// pub struct RouterRules {
//     cfg: Config,
//     cluster: ClusterRef,
// }
//
// impl RouterRules {
//     pub fn create(cfg: Config, cluster: ClusterRef) -> Self {
//         RouterRules { cfg, cluster }
//     }
//
//     pub fn router(&self) -> Result<Router<BoxRoute<hyper::body::Body, Infallible>>>
// where {
//         let app = Router::new()
//             .route("/v1/hello", get(super::v1::hello::hello_handler))
//             .route("/v1/hello", get(super::v1::hello::hello_handler))
//             .layer(AddExtensionLayer::new(self.cfg.clone()));
//         Ok(app.boxed())
//     }
// }

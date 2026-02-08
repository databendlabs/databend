// Copyright 2021 Datafuse Labs
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

use std::net::SocketAddr;
use std::sync::Arc;

use anyerror::AnyError;
use databend_base::shutdown::Graceful;
use databend_common_http::HttpError;
use databend_common_http::HttpShutdownHandler;
use databend_common_http::health_handler;
use databend_common_http::home::debug_home_handler;
#[cfg(feature = "memory-profiling")]
use databend_common_http::jeprof::debug_jeprof_dump_handler;
use databend_common_http::pprof::debug_pprof_handler;
use databend_meta::meta_node::meta_handle::MetaHandle;
use databend_meta_runtime_api::SpawnApi;
use futures::future::BoxFuture;
use log::info;
use log::warn;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use poem::get;
use poem::listener::OpensslTlsConfig;

use crate::HttpServiceConfig;
use crate::v1::ctrl::TransferLeaderQuery;
use crate::v1::features::SetFeatureQuery;

pub struct HttpService<SP: SpawnApi> {
    cfg: HttpServiceConfig,
    shutdown_handler: HttpShutdownHandler,
    version: String,
    pub(crate) meta_handle: Arc<MetaHandle<SP>>,
}

impl<SP: SpawnApi> HttpService<SP> {
    pub fn create(
        cfg: HttpServiceConfig,
        version: String,
        meta_handle: Arc<MetaHandle<SP>>,
    ) -> Box<Self> {
        Box::new(HttpService {
            cfg,
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
            version,
            meta_handle,
        })
    }

    pub fn build_router(&self) -> impl Endpoint + use<SP> {
        let mh = self.meta_handle.clone();

        #[cfg_attr(not(feature = "memory-profiling"), allow(unused_mut))]
        let mut route = Route::new()
            .at("/v1/health", get(health_handler))
            .at("/v1/config", get(crate::v1::config::config_handler))
            .at("/v1/ctrl/trigger_snapshot", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |_req: Request| {
                    let mh = mh.clone();
                    async move { Self::trigger_snapshot(mh).await }
                }))
            })
            .at("/v1/ctrl/trigger_transfer_leader", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |req: Request| {
                    let mh = mh.clone();
                    async move {
                        let query: Option<TransferLeaderQuery> = req
                            .uri()
                            .query()
                            .and_then(|q| serde_urlencoded::from_str(q).ok());
                        Self::trigger_transfer_leader(mh, query).await
                    }
                }))
            })
            .at("/v1/features/list", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |_req: Request| {
                    let mh = mh.clone();
                    async move { Self::features_list(mh).await }
                }))
            })
            .at("/v1/features/set", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |req: Request| {
                    let mh = mh.clone();
                    async move {
                        let query: Option<SetFeatureQuery> = req
                            .uri()
                            .query()
                            .and_then(|q| serde_urlencoded::from_str(q).ok());
                        Self::features_set(mh, query).await
                    }
                }))
            })
            .at("/v1/cluster/nodes", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |_req: Request| {
                    let mh = mh.clone();
                    async move { Self::nodes_handler(mh).await }
                }))
            })
            .at("/v1/cluster/status", {
                let mh = mh.clone();
                let version = self.version.clone();
                get(poem::endpoint::make(move |_req: Request| {
                    let mh = mh.clone();
                    let version = version.clone();
                    async move { Self::status_handler(mh, &version).await }
                }))
            })
            .at("/v1/metrics", {
                let mh = mh.clone();
                get(poem::endpoint::make(move |_req: Request| {
                    let mh = mh.clone();
                    async move { Self::metrics_handler(mh).await }
                }))
            })
            .at("/debug/home", get(debug_home_handler))
            .at("/debug/pprof/profile", get(debug_pprof_handler));

        #[cfg(feature = "memory-profiling")]
        {
            route = route.at(
                // to follow the conversions of jeprof, we arrange the path in
                // this way, so that jeprof could be invoked like:
                //   `jeprof ./target/debug/databend-meta http://localhost:28002/debug/mem`
                // and jeprof will translate the above url into sth like:
                //    "http://localhost:28002/debug/mem/pprof/profile?seconds=30"
                "/debug/mem/pprof/profile",
                get(debug_jeprof_dump_handler),
            );
        };
        route.data(self.cfg.clone())
    }

    fn build_tls(config: &HttpServiceConfig) -> OpensslTlsConfig {
        OpensslTlsConfig::new()
            .cert_from_file(config.admin.tls.cert.as_str())
            .key_from_file(config.admin.tls.key.as_str())
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<(), HttpError> {
        info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.cfg);
        self.shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router(), None)
            .await?;
        Ok(())
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<(), HttpError> {
        warn!("Http API TLS not set");

        self.shutdown_handler
            .start_service(listening, None, self.build_router(), None)
            .await?;
        Ok(())
    }

    pub async fn do_start(&mut self) -> Result<(), HttpError> {
        let conf = self.cfg.clone();
        let listening = conf
            .admin
            .api_address
            .parse::<SocketAddr>()
            .map_err(|e| HttpError::BadAddressFormat(AnyError::new(&e)))?;

        if conf.admin.tls.enabled() {
            self.start_with_tls(listening).await
        } else {
            self.start_without_tls(listening).await
        }
    }

    pub async fn do_stop(&mut self, force: Option<BoxFuture<'static, ()>>) {
        self.shutdown_handler.stop(force).await;
    }
}

#[async_trait::async_trait]
impl<SP: SpawnApi> Graceful for HttpService<SP> {
    type Error = AnyError;

    async fn shutdown(&mut self, force: Option<BoxFuture<'static, ()>>) -> Result<(), Self::Error> {
        self.do_stop(force).await;
        Ok(())
    }
}

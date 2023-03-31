// Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use common_auth::RefreshableToken;
use common_base::base::GlobalInstance;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::share::GetShareEndpointReq;
use common_meta_app::share::ShareEndpointMeta;
use common_meta_app::share::TableInfoMap;
use common_storage::ShareTableConfig;
use common_users::UserApiProvider;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;
use parking_lot::RwLock;
use tracing::error;
use tracing::info;

use crate::signer::TENANT_HEADER;

struct EndpointConfig {
    pub url: String,
    pub token: RefreshableToken,
}

pub struct ShareEndpointManager {
    endpoint_map: Arc<RwLock<BTreeMap<String, ShareEndpointMeta>>>,

    client: HttpClient,
}

impl ShareEndpointManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(ShareEndpointManager {
            endpoint_map: Arc::new(RwLock::new(BTreeMap::new())),
            client: HttpClient::new()?,
        });
        Ok(())
    }

    pub fn instance() -> Arc<ShareEndpointManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    async fn get_share_endpoint(
        &self,
        from_tenant: &str,
        to_tenant: &str,
    ) -> Result<ShareEndpointMeta> {
        let endpoint_meta = {
            let endpoint_map = self.endpoint_map.read();
            endpoint_map.get(to_tenant).cloned()
        };

        match endpoint_meta {
            Some(endpoint_meta) => {
                return Ok(endpoint_meta);
            }
            None => {
                let req = GetShareEndpointReq {
                    tenant: from_tenant.to_owned(),
                    endpoint: None,
                    to_tenant: Some(to_tenant.to_owned()),
                };
                let meta_api = UserApiProvider::instance().get_meta_store_client();
                let resp = meta_api.get_share_endpoint(req).await?;
                if let Some((_, endpoint_meta)) = resp.share_endpoint_meta_vec.into_iter().next() {
                    let mut endpoint_map = self.endpoint_map.write();
                    endpoint_map.insert(to_tenant.to_owned(), endpoint_meta.clone());
                    return Ok(endpoint_meta);
                }
            }
        };

        Err(ErrorCode::EmptyShareEndpointConfig(format!(
            "No ShareEndpoint to tenant {:?}",
            to_tenant
        )))
    }

    #[async_backtrace::framed]
    pub async fn get_table_info_map(
        &self,
        from_tenant: &str,
        db_info: &DatabaseInfo,
        tables: Vec<String>,
    ) -> Result<TableInfoMap> {
        let to_tenant = &db_info.meta.from_share.as_ref().unwrap().tenant;
        let is_same_tenant = from_tenant == to_tenant;

        // fail to try again if there is some endpoint, cause endpoint may be changed.
        let mut need_try_again = {
            if is_same_tenant {
                false
            } else {
                let endpoint_map = self.endpoint_map.read();
                endpoint_map.contains_key(to_tenant)
            }
        };
        let mut last_endpoint_meta = None;
        let mut last_error = None;

        loop {
            let endpoint_config = if is_same_tenant {
                // If share from the same tenant, query from share table config
                let url = ShareTableConfig::share_endpoint_address();
                match url {
                    Some(url) => EndpointConfig {
                        url,
                        token: ShareTableConfig::share_endpoint_token(),
                    },
                    None => {
                        return Err(ErrorCode::EmptyShareEndpointConfig(format!(
                            "No ShareEndpoint to tenant {:?}",
                            to_tenant
                        )));
                    }
                }
            } else {
                // Else get share endpoint from meta API
                let endpoint_meta = self.get_share_endpoint(from_tenant, to_tenant).await?;
                match last_endpoint_meta {
                    Some(ref last_endpoint_meta) => {
                        if last_endpoint_meta == &endpoint_meta {
                            // If endpoint meta is the same, no need to try again.
                            return Err(ErrorCode::EmptyShareEndpointConfig(format!(
                                "Query ShareEndpoint to tenant {:?} from endpoint {:?} fail: {:?}",
                                to_tenant, endpoint_meta, last_error,
                            )));
                        }
                    }
                    None => {
                        last_endpoint_meta = Some(endpoint_meta.clone());
                    }
                }

                EndpointConfig {
                    url: endpoint_meta.url,
                    token: RefreshableToken::Direct(from_tenant.to_owned()),
                }
            };

            let bs = Bytes::from(serde_json::to_vec(&tables)?);
            let auth = endpoint_config.token.to_header().await?;
            let requester = GlobalConfig::instance().as_ref().query.tenant_id.clone();
            let req = Request::builder()
                .method(Method::POST)
                .uri(&endpoint_config.url)
                .header(AUTHORIZATION, auth)
                .header(CONTENT_LENGTH, bs.len())
                .header(TENANT_HEADER, requester)
                .body(AsyncBody::Bytes(bs))?;
            let resp = self.client.send_async(req).await;
            match resp {
                Ok(resp) => {
                    let bs = resp.into_body().bytes().await?;
                    let table_info_map: TableInfoMap = serde_json::from_slice(&bs)?;

                    return Ok(table_info_map);
                }
                Err(err) => {
                    if !need_try_again {
                        error!("get_table_info_map error: {:?}", err);
                        return Err(err.into());
                    } else {
                        info!("get_table_info_map error: {:?}, try again", err);
                        last_error = Some(err);
                        // endpoint may be changed, so cleanup endpoint and try again
                        need_try_again = false;
                        let mut endpoint_map = self.endpoint_map.write();
                        endpoint_map.remove(to_tenant);
                    }
                }
            }
        }
    }
}

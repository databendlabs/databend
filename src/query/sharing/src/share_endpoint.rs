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
use common_meta_app::share::ShareNameIdent;
use common_meta_app::share::ShareSpec;
use common_meta_app::share::TableInfoMap;
use common_users::UserApiProvider;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;
use tracing::error;

use crate::signer::TENANT_HEADER;

struct EndpointConfig {
    pub url: String,
    pub token: RefreshableToken,
}

pub struct ShareEndpointManager {
    client: HttpClient,
}

impl ShareEndpointManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(ShareEndpointManager {
            client: HttpClient::new()?,
        }));
        Ok(())
    }

    pub fn instance() -> Arc<ShareEndpointManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    async fn get_share_endpoint(
        &self,
        from_tenant: &str,
        to_tenant: Option<String>,
    ) -> Result<Vec<ShareEndpointMeta>> {
        let req = GetShareEndpointReq {
            tenant: from_tenant.to_owned(),
            endpoint: None,
            to_tenant,
        };
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let resp = meta_api.get_share_endpoint(req).await?;
        let mut share_endpoint_vec = Vec::with_capacity(resp.share_endpoint_meta_vec.len());
        for (_, endpoint_meta) in resp.share_endpoint_meta_vec.iter() {
            share_endpoint_vec.push(endpoint_meta.clone());
        }
        Ok(share_endpoint_vec)
    }

    #[async_backtrace::framed]
    pub async fn get_table_info_map(
        &self,
        from_tenant: &str,
        db_info: &DatabaseInfo,
        tables: Vec<String>,
    ) -> Result<TableInfoMap> {
        let to_tenant = &db_info.meta.from_share.as_ref().unwrap().tenant;
        let share_name = &db_info.meta.from_share.as_ref().unwrap().share_name;

        let endpoint_config = {
            let endpoint_meta_vec = self
                .get_share_endpoint(from_tenant, Some(to_tenant.clone()))
                .await?;
            match endpoint_meta_vec.get(0) {
                Some(endpoint_meta) => EndpointConfig {
                    url: endpoint_meta.url.clone(),
                    token: RefreshableToken::Direct(from_tenant.to_owned()),
                },
                None => {
                    return Err(ErrorCode::UnknownShareEndpoint(format!(
                        "UnknownShareEndpoint from {:?} to {:?}",
                        from_tenant, to_tenant
                    )));
                }
            }
        };

        let url = format!(
            "{}tenant/{}/{}/meta",
            endpoint_config.url, to_tenant, share_name
        );
        let bs = Bytes::from(serde_json::to_vec(&tables)?);
        let auth = endpoint_config.token.to_header().await?;
        let requester = GlobalConfig::instance().as_ref().query.tenant_id.clone();
        let req = Request::builder()
            .method(Method::POST)
            .uri(&url)
            .header(AUTHORIZATION, auth)
            .header(CONTENT_LENGTH, bs.len())
            .header(TENANT_HEADER, requester)
            .body(AsyncBody::Bytes(bs))?;
        let resp = self.client.send_async(req).await;
        match resp {
            Ok(resp) => {
                let bs = resp.into_body().bytes().await?;
                let table_info_map: TableInfoMap = serde_json::from_slice(&bs)?;

                Ok(table_info_map)
            }
            Err(err) => Err(err.into()),
        }
    }

    #[async_backtrace::framed]
    pub async fn get_inbound_shares(
        &self,
        from_tenant: &str,
        to_tenant: Option<String>,
        share_name: Option<ShareNameIdent>,
    ) -> Result<Vec<(String, ShareSpec)>> {
        match self.get_share_endpoint(from_tenant, to_tenant).await {
            Err(_) => Ok(vec![]),
            Ok(endpoint_meta_vec) => {
                let mut share_spec_vec = vec![];
                let share_names: Vec<String> = vec![];
                for endpoint_meta in endpoint_meta_vec {
                    let endpoint_config = EndpointConfig {
                        url: endpoint_meta.url.clone(),
                        token: RefreshableToken::Direct(from_tenant.to_owned()),
                    };
                    let url = format!("{}tenant/{}/share_spec", endpoint_meta.url, from_tenant);
                    let bs = Bytes::from(serde_json::to_vec(&share_names)?);
                    let auth = endpoint_config.token.to_header().await?;
                    let requester = GlobalConfig::instance().as_ref().query.tenant_id.clone();
                    let req = Request::builder()
                        .method(Method::POST)
                        .uri(&url)
                        .header(AUTHORIZATION, auth)
                        .header(CONTENT_LENGTH, bs.len())
                        .header(TENANT_HEADER, requester)
                        .body(AsyncBody::Bytes(bs))?;
                    let resp = self.client.send_async(req).await;
                    match resp {
                        Ok(resp) => {
                            let bs = resp.into_body().bytes().await?;
                            let ret: Vec<ShareSpec> = serde_json::from_slice(&bs)?;
                            for share_spec in ret {
                                if let Some(ref share_name) = share_name {
                                    if share_spec.name == share_name.share_name
                                        && endpoint_meta.tenant == share_name.tenant
                                    {
                                        share_spec_vec
                                            .push((endpoint_meta.tenant.clone(), share_spec));
                                        return Ok(share_spec_vec);
                                    }
                                }
                                share_spec_vec.push((endpoint_meta.tenant.clone(), share_spec));
                            }
                        }
                        Err(err) => {
                            error!("get_inbound_shares error: {:?}", err);
                            continue;
                        }
                    }
                }

                Ok(share_spec_vec)
            }
        }
    }
}

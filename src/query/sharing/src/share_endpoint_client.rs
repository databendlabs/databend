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

use std::collections::BTreeMap;

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::share::ShareCredential;
use databend_common_meta_app::share::ShareEndpointMeta;
use databend_common_meta_app::share::ShareSpec;
use log::error;
use reqwest::header::HeaderMap;
use ring::hmac;

use crate::signer::AUTH_METHOD_HEADER;
use crate::signer::HMAC_AUTH_METHOD;
use crate::signer::SIGNATURE_HEADER;
use crate::signer::TENANT_HEADER;

pub struct ShareEndpointClient {}

impl ShareEndpointClient {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate_auth_headers(
        path: &str,
        credential: &ShareCredential,
        from_tenant: &str,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_HEADER, from_tenant.parse().unwrap());
        match credential {
            ShareCredential::HMAC(hmac_credential) => {
                let key = hmac::Key::new(
                    hmac::HMAC_SHA256,
                    hmac_credential.key.as_bytes().to_vec().as_ref(),
                );
                headers.insert(AUTH_METHOD_HEADER, HMAC_AUTH_METHOD.parse().unwrap());
                let auth = format!("{}@{}", from_tenant, path);
                let signature = hmac::sign(&key, auth.as_bytes());
                let signature = URL_SAFE.encode(signature.as_ref());
                headers.insert(SIGNATURE_HEADER, signature.parse().unwrap());
            }
        }
        headers
    }

    #[async_backtrace::framed]
    pub async fn get_share_spec_by_name(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
    ) -> Result<ShareSpec> {
        let path = format!("/tenant/{}/{}/share_spec", to_tenant, share_name);
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };

        let client = reqwest::Client::new();
        let resp = client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: ShareSpec = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_table_by_name(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let path = format!(
            "/tenant/{}/{}/table/{}/share_table",
            to_tenant, share_name, table_name
        );
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };
        let client = reqwest::Client::new();
        let resp = client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: TableInfo = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_tables(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
    ) -> Result<BTreeMap<String, TableInfo>> {
        let path = format!("/tenant/{}/{}/share_tables", to_tenant, share_name);
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };
        let client = reqwest::Client::new();
        let resp = client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: BTreeMap<String, TableInfo> = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }
}

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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use cookie::Cookie;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use reqwest::Client;
use reqwest::ClientBuilder;
use reqwest::cookie::CookieStore;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

struct GlobalCookieStore {
    cookies: RwLock<HashMap<String, Cookie<'static>>>,
}

impl GlobalCookieStore {
    pub fn new() -> Self {
        GlobalCookieStore {
            cookies: RwLock::new(HashMap::new()),
        }
    }
}

impl CookieStore for GlobalCookieStore {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, _url: &Url) {
        let iter = cookie_headers
            .filter_map(|val| std::str::from_utf8(val.as_bytes()).ok())
            .filter_map(|kv| Cookie::parse(kv).map(|c| c.into_owned()).ok());

        let mut guard = self.cookies.write().unwrap();
        for cookie in iter {
            guard.insert(cookie.name().to_string(), cookie);
        }
    }

    fn cookies(&self, _url: &Url) -> Option<HeaderValue> {
        let guard = self.cookies.read().unwrap();
        let s: String = guard
            .values()
            .map(|cookie| cookie.name_value())
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");

        if s.is_empty() {
            return None;
        }

        HeaderValue::from_str(&s).ok()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub(crate) struct ServerInfo {
    pub(crate) id: String,
    pub(crate) start_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct HttpSessionConf {
    pub(crate) database: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) secondary_roles: Option<Vec<String>>,
    pub(crate) settings: Option<BTreeMap<String, String>>,
    pub(crate) txn_state: Option<String>,
    pub(crate) last_server_info: Option<ServerInfo>,
    #[serde(default)]
    pub(crate) last_query_ids: Vec<String>,
}

#[derive(serde::Deserialize, Debug)]
pub(crate) struct QueryResponse {
    pub(crate) session: Option<HttpSessionConf>,
    pub(crate) data: Option<serde_json::Value>,
    next_uri: Option<String>,

    pub(crate) error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct TokenInfo {
    session_token: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    tokens: Option<TokenInfo>,
}

pub(crate) struct HttpClient {
    pub(crate) host: String,
    pub(crate) client: Client,
    pub(crate) session_token: String,
    pub(crate) session: Option<HttpSessionConf>,
}

impl HttpClient {
    pub(crate) async fn create(host: String, username: String, password: String) -> Result<Self> {
        let mut header = HeaderMap::new();
        header.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        let cookie_provider = GlobalCookieStore::new();
        let cookie = HeaderValue::from_str("cookie_enabled=true").unwrap();
        let mut initial_cookies = [&cookie].into_iter();
        cookie_provider.set_cookies(&mut initial_cookies, &Url::parse("https://a.com").unwrap());
        let client = ClientBuilder::new()
            .cookie_provider(Arc::new(cookie_provider))
            .default_headers(header)
            // https://github.com/hyperium/hyper/issues/2136#issuecomment-589488526
            .http2_keep_alive_timeout(Duration::from_secs(15))
            .pool_max_idle_per_host(0)
            .build()?;

        let url = format!("{}/v1/session/login", host);

        let login_resp = client
            .post(&url)
            .body("{}")
            .basic_auth(username, Some(password))
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<LoginResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?;
        let session_token = match login_resp.tokens {
            Some(tokens) => tokens.session_token,
            None => {
                return Err(ErrorCode::AuthenticateFailure(
                    "failed to get session token",
                ));
            }
        };

        Ok(Self {
            host,
            client,
            session_token,
            session: None,
        })
    }

    pub(crate) async fn query(&mut self, sql: &str) -> Result<Vec<QueryResponse>> {
        let url = format!("{}/v1/query", self.host);
        let mut responses = vec![];
        let response = self.post_query(sql, &url).await?;
        let mut next_uri_opt = response.next_uri.clone();
        responses.push(response);
        while let Some(next_uri) = &next_uri_opt {
            let url = format!("{}{}", self.host, next_uri);
            let new_response = self.poll_query_result(&url).await?;
            if new_response.session.is_some() {
                self.session = new_response.session.clone();
            }
            next_uri_opt = new_response.next_uri.clone();
            responses.push(new_response);
        }
        Ok(responses)
    }

    // Send request and get response by json format
    async fn post_query(&self, sql: &str, url: &str) -> Result<QueryResponse> {
        let mut query = HashMap::new();
        query.insert("sql", serde_json::to_value(sql)?);
        if let Some(session) = &self.session {
            query.insert("session", serde_json::to_value(session)?);
        }

        Ok(self
            .client
            .post(url)
            .json(&query)
            .bearer_auth(&self.session_token)
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<QueryResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?)
    }

    async fn poll_query_result(&self, url: &str) -> Result<QueryResponse> {
        Ok(self
            .client
            .get(url)
            .bearer_auth(&self.session_token)
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<QueryResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?)
    }
}

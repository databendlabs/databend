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

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use databend_common_base::headers::HEADER_SESSION;
use databend_common_base::headers::HEADER_SESSION_ID;
use databend_common_meta_app::tenant::Tenant;
use http::HeaderMap;
use jwt_simple::prelude::Deserialize;
use jwt_simple::prelude::Serialize;
use log::info;
use log::warn;
use poem::web::cookie::Cookie;
use poem::web::cookie::CookieJar;
use poem::Request;
use poem::Response;
use serde::Deserializer;
use serde::Serializer;
use uuid::Uuid;

use crate::servers::http::middleware::ClientCapabilities;
use crate::servers::http::v1::unix_ts;
use crate::servers::http::v1::ClientSessionManager;

const COOKIE_LAST_REFRESH_TIME: &str = "last_refresh_time";
const COOKIE_SESSION_ID: &str = "session_id";
const COOKIE_COOKIE_ENABLED: &str = "cookie_enabled";

fn make_cookie(name: impl Into<String>, value: impl Into<String>) -> Cookie {
    let mut cookie = Cookie::new_with_str(name, value);
    cookie.set_path("/");
    cookie
}

// migrating from cookie to custom header:
// request:
//  try read in order 1. custom header 2. cookie
// response:
//  always custom header
//  cookie only if enabled
#[derive(Clone)]
pub struct ClientSession {
    pub header: ClientSessionHeader,

    pub use_cookie: bool,
    pub is_new_session: bool,
    pub refreshed: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ClientSessionHeader {
    pub id: String,
    #[serde(serialize_with = "serialize_ts", deserialize_with = "deserialize_ts")]
    pub last_refresh_time: SystemTime,
}

impl ClientSessionHeader {
    fn encode(&self) -> String {
        let s = serde_json::to_string(&self).unwrap();
        URL_SAFE.encode(&s)
    }
}

impl ClientSession {
    pub fn try_decode(
        req: &Request,
        caps: &mut ClientCapabilities,
    ) -> Result<Option<ClientSession>, String> {
        if let Some(s) = Self::from_custom_header(req.headers(), caps)? {
            Ok(Some(s))
        } else if let Some(s) = Self::from_cookie(req.cookie(), caps)? {
            Ok(Some(s))
        } else {
            Ok(None)
        }
    }

    fn new_session(use_cookie: bool) -> Self {
        let id = Uuid::now_v7().to_string();
        info!("[HTTP-SESSION] Created new session with ID: {}", id);
        ClientSession {
            header: ClientSessionHeader {
                id,
                last_refresh_time: SystemTime::now(),
            },
            use_cookie,
            is_new_session: true,
            refreshed: false,
        }
    }

    fn old_session(use_cookie: bool, header: ClientSessionHeader) -> Self {
        ClientSession {
            header,
            use_cookie,
            is_new_session: false,
            refreshed: false,
        }
    }

    fn from_custom_header(
        headers: &HeaderMap,
        caps: &mut ClientCapabilities,
    ) -> Result<Option<ClientSession>, String> {
        if let Some(v) = headers.get(HEADER_SESSION) {
            caps.session_header = true;
            let v = v.to_str().unwrap().to_string().trim().to_owned();
            let s = if v.is_empty() {
                // note that curl -H "X-xx:" not work
                Self::new_session(false)
            } else {
                let json = URL_SAFE.decode(&v).map_err(|e| {
                    format!(
                        "Invalid value {} for X-DATABEND-SESSION, base64 decode error: {}",
                        v, e
                    )
                })?;
                let header = serde_json::from_slice(&json).map_err(|e| {
                    format!(
                        "Invalid value {} for X-DATABEND-SESSION, JSON decode error: {}",
                        v, e
                    )
                })?;
                Self::old_session(false, header)
            };
            Ok(Some(s))
        } else if caps.session_header {
            Ok(Some(Self::new_session(false)))
        } else {
            Ok(None)
        }
    }

    fn from_cookie(
        cookie: &CookieJar,
        caps: &mut ClientCapabilities,
    ) -> Result<Option<ClientSession>, String> {
        let cookie_enabled = cookie.get(COOKIE_COOKIE_ENABLED).is_some() || caps.session_cookie;
        if cookie_enabled {
            let s = if let Some(sid) = cookie.get(COOKIE_SESSION_ID) {
                let id = sid.value_str().to_string();
                let last_access_time = match cookie.get(COOKIE_LAST_REFRESH_TIME) {
                    None => SystemTime::now(),
                    Some(v) => {
                        let ts: u64 = v
                            .value_str()
                            .parse()
                            .map_err(|e| format!("Invalid last access time value {}: {}", v, e))?;
                        UNIX_EPOCH
                            .checked_add(std::time::Duration::from_secs(ts))
                            .ok_or_else(|| {
                                format!("last_access_time {ts} overflows system time limits")
                            })?
                    }
                };

                let header = ClientSessionHeader {
                    id,
                    last_refresh_time: last_access_time,
                };
                Self::old_session(true, header)
            } else {
                let s = Self::new_session(true);
                cookie.add(make_cookie(COOKIE_SESSION_ID, &s.header.id));
                cookie.add(make_cookie(
                    COOKIE_LAST_REFRESH_TIME,
                    unix_ts().as_secs().to_string().as_str(),
                ));
                s
            };
            Ok(Some(s))
        } else {
            Ok(None)
        }
    }

    pub async fn try_refresh_state(
        &mut self,
        tenant: Tenant,
        user_name: &str,
        cookie: &CookieJar,
        is_worksheet: bool,
    ) -> databend_common_exception::Result<()> {
        let client_session_mgr = ClientSessionManager::instance();
        match self.header.last_refresh_time.elapsed() {
            Ok(elapsed) => {
                // worksheet
                if is_worksheet || elapsed > client_session_mgr.min_refresh_interval {
                    if client_session_mgr.refresh_in_memory_states(&self.header.id, user_name) {
                        client_session_mgr
                            .refresh_session_handle(tenant, user_name.to_string(), &self.header.id)
                            .await?;
                        info!(
                            "[HTTP-SESSION] refreshing session {} after {} seconds",
                            self.header.id,
                            elapsed.as_secs(),
                        );
                    }
                    self.refreshed = true;
                    if self.use_cookie {
                        cookie.add(make_cookie(
                            COOKIE_LAST_REFRESH_TIME,
                            unix_ts().as_secs().to_string().as_str(),
                        ));
                    }
                };
            }
            Err(err) => {
                warn!(
                        "[HTTP-SESSION] Invalid last_refresh_time: detected clock drift or incorrect timestamp, difference: {:?}",
                        err.duration()
                    );
            }
        }
        Ok(())
    }

    pub fn on_response(&self, resp: &mut Response) {
        let mut header = self.header.clone();
        if self.refreshed {
            header.last_refresh_time = SystemTime::now();
        }
        resp.headers_mut()
            .insert(HEADER_SESSION_ID, header.id.parse().unwrap());
        resp.headers_mut()
            .insert(HEADER_SESSION, header.encode().parse().unwrap());
    }
}

fn serialize_ts<S>(value: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    let ts = value
        .duration_since(UNIX_EPOCH)
        .map_err(|e| {
            serde::ser::Error::custom(format!(
                "Bad SystemTime {value:?} for last_refresh_time: {e}"
            ))
        })?
        .as_secs();
    serializer.serialize_u64(ts)
}

fn deserialize_ts<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
where D: Deserializer<'de> {
    let timestamp = u64::deserialize(deserializer)?;
    UNIX_EPOCH
        .checked_add(Duration::from_secs(timestamp))
        .ok_or_else(|| {
            serde::de::Error::custom(format!(
                "last_refresh_time {timestamp} overflows system time limits"
            ))
        })
}

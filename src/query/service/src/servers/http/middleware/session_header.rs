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

// Logs from this module will show up as "[HTTP-SESSION] ...".
databend_common_tracing::register_module_tag!("[HTTP-SESSION]");

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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

use crate::servers::http::middleware::json_header::decode_json_header;
use crate::servers::http::middleware::json_header::encode_json_header;
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

#[derive(Clone, Eq, PartialEq)]
pub enum ClientSessionType {
    // currently use in drivers
    Cookie,
    // to be used when cookie is inconvenient
    CustomHeader,
    // for now used only for worksheet
    IDOnly,
}

#[derive(Clone)]
pub struct ClientSession {
    pub header: ClientSessionHeader,
    pub typ: ClientSessionType,
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
        encode_json_header(self)
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

    pub fn try_decode_for_worksheet(req: &Request) -> Option<Self> {
        if let Some(v) = req.headers().get(HEADER_SESSION_ID) {
            let id = v.to_str().ok()?.to_string().trim().to_owned();
            if !id.is_empty() {
                return Some(ClientSession {
                    header: ClientSessionHeader {
                        id,
                        last_refresh_time: SystemTime::now(),
                    },
                    typ: ClientSessionType::IDOnly,
                    is_new_session: false,
                    refreshed: false,
                });
            }
        }
        None
    }

    fn new_session(typ: ClientSessionType) -> Self {
        let id = Uuid::now_v7().to_string();
        info!("[HTTP-SESSION] Created new session with ID: {}", id);
        ClientSession {
            header: ClientSessionHeader {
                id,
                last_refresh_time: SystemTime::now(),
            },
            typ,
            is_new_session: true,
            refreshed: false,
        }
    }

    fn old_session(typ: ClientSessionType, header: ClientSessionHeader) -> Self {
        ClientSession {
            header,
            typ,
            is_new_session: false,
            refreshed: false,
        }
    }

    fn from_custom_header(
        headers: &HeaderMap,
        caps: &mut ClientCapabilities,
    ) -> Result<Option<ClientSession>, String> {
        if caps.session_header {
            if let Some(v) = headers.get(HEADER_SESSION) {
                let v = v
                    .to_str()
                    .map_err(|_| "Invalid header value")?
                    .to_string()
                    .trim()
                    .to_owned();
                if !v.is_empty() {
                    let header = decode_json_header(HEADER_SESSION, &v)?;
                    return Ok(Some(Self::old_session(
                        ClientSessionType::CustomHeader,
                        header,
                    )));
                };
            }
            Ok(Some(Self::new_session(ClientSessionType::CustomHeader)))
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
                Self::old_session(ClientSessionType::Cookie, header)
            } else {
                let s = Self::new_session(ClientSessionType::Cookie);
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
    ) -> databend_common_exception::Result<()> {
        let client_session_mgr = ClientSessionManager::instance();
        match self.header.last_refresh_time.elapsed() {
            Ok(elapsed) => {
                if ClientSessionType::IDOnly == self.typ
                    || elapsed > client_session_mgr.min_refresh_interval
                {
                    client_session_mgr
                        .try_refresh_state(tenant, &self.header.id, user_name)
                        .await?;
                    self.refreshed = true;
                    if ClientSessionType::Cookie == self.typ {
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
        if let Ok(session_id_value) = header.id.parse() {
            resp.headers_mut()
                .insert(HEADER_SESSION_ID, session_id_value);
        }
        if ClientSessionType::CustomHeader == self.typ {
            if self.refreshed {
                header.last_refresh_time = SystemTime::now();
            }
            if let Ok(session_value) = header.encode().parse() {
                resp.headers_mut().insert(HEADER_SESSION, session_value);
            }
        }
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

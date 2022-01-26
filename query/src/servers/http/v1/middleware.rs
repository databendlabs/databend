// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserInfo;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use headers::HeaderMap;
use hyper::http::header::AUTHORIZATION;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::Endpoint;
use poem::Middleware;
use poem::Request;

use crate::sessions::SessionManager;
use crate::users::auth::auth_mgr::Credential;

pub struct HTTPSessionMiddleware {
    pub session_manager: Arc<SessionManager>,
}

fn get_credential(headers: &HeaderMap) -> Result<Option<Credential>> {
    let auth_headers: Vec<_> = headers.get_all(AUTHORIZATION).iter().collect();
    if auth_headers.len() > 1 {
        let msg = &format!("Multiple {} headers detected", AUTHORIZATION);
        return Err(ErrorCode::AuthenticateFailure(msg));
    }
    if auth_headers.is_empty() {
        return Ok(None);
    }
    let value = auth_headers[0];
    if value.as_bytes().starts_with(b"Basic ") {
        match Basic::decode(value) {
            Some(basic) => {
                let name = basic.username().to_string();
                let password = basic.password().to_owned().as_bytes().to_vec();
                let password = (!password.is_empty()).then_some(password);
                let c = Credential::Password {
                    name,
                    password,
                    hostname: None,
                };
                Ok(Some(c))
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Basic auth header")),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => Ok(Some(Credential::Jwt {
                token: bearer.token().to_string(),
            })),
            None => Err(ErrorCode::AuthenticateFailure("bad Bearer auth header")),
        }
    } else {
        Ok(None)
    }
}

impl<E: Endpoint> Middleware<E> for HTTPSessionMiddleware {
    type Output = HTTPSessionEndpoint<E>;
    fn transform(&self, ep: E) -> Self::Output {
        HTTPSessionEndpoint {
            ep,
            manager: self.session_manager.clone(),
        }
    }
}

pub struct HTTPSessionEndpoint<E> {
    ep: E,
    manager: Arc<SessionManager>,
}

impl<E> HTTPSessionEndpoint<E> {
    async fn auth(&self, req: &Request) -> Result<UserInfo> {
        let credential = get_credential(req.headers())?;
        match credential {
            Some(c) => self.manager.get_auth_manager().auth(&c).await,
            None => self.manager.get_auth_manager().no_auth().await,
        }
    }
}

#[poem::async_trait]
impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = E::Output;

    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        match self.auth(&req).await {
            Ok(user_info) => {
                req.extensions_mut().insert(self.manager.clone());
                req.extensions_mut().insert(user_info);
                return self.ep.call(req).await;
            }
            Err(err) => Err(PoemError::from_string(
                err.message(),
                StatusCode::UNAUTHORIZED,
            )),
        }
    }
}

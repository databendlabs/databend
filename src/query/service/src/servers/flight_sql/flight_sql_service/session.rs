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

use std::sync::Arc;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_users::UserApiProvider;
use tonic::Request;
use tonic::Status;
use tonic::metadata::MetadataMap;

use super::status;
use crate::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use crate::sessions::Session;
use crate::sessions::SessionManager;

impl FlightSqlServiceImpl {
    pub(super) fn get_session<T>(&self, req: &Request<T>) -> Result<Arc<Session>, Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("No authorization header!"))?;
        let str = auth
            .to_str()
            .map_err(|e| Status::unauthenticated(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::unauthenticated("Invalid auth header!"))?;
        }
        let session_id = authorization[bearer.len()..].to_string();

        if let Some(session) = self.sessions.lock().get(&session_id) {
            Ok(session)
        } else {
            Err(Status::unauthenticated(format!(
                "session_id not found: {session_id}"
            )))?
        }
    }

    pub(super) fn get_header_value(metadata: &MetadataMap, key: &str) -> Option<String> {
        metadata
            .get(key)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string())
    }

    pub(super) fn get_user_password(metadata: &MetadataMap) -> Result<(String, String), String> {
        let basic = "Basic ";
        let authorization = Self::get_header_value(metadata, "authorization")
            .ok_or_else(|| "authorization not parsable".to_string())?;

        if !authorization.starts_with(basic) {
            return Err(format!("Auth type not implemented: {authorization}"));
        }
        let base64 = &authorization[basic.len()..];
        let bytes = BASE64_STANDARD
            .decode(base64)
            .map_err(|e| format!("authorization not decodable: {}", e))?;
        let str =
            String::from_utf8(bytes).map_err(|e| format!("authorization not parsable: {}", e))?;
        let parts: Vec<_> = str.split(':').collect();
        let (user, pass) = match parts.as_slice() {
            [user, pass] => (user, pass),
            _ => return Err("Invalid authorization header".to_string()),
        };
        Ok((user.to_string(), pass.to_string()))
    }

    #[async_backtrace::framed]
    pub(super) async fn auth_user_password(
        user: String,
        password: String,
        client_ip: Option<&str>,
    ) -> Result<Arc<Session>, Status> {
        let user_api = UserApiProvider::instance();
        let session_manager = SessionManager::instance();
        let session = session_manager
            .create_session(SessionType::FlightSQL)
            .await
            .map_err(|e| status!("Could not create session", e))?;

        let session = session_manager.register_session(session)?;

        let tenant = session.get_current_tenant();

        let identity = UserIdentity::new(&user, "%");
        let global_network_policy = session
            .get_settings()
            .get_network_policy()
            .unwrap_or_default();
        let password = password.as_bytes().to_vec();
        let password = (!password.is_empty()).then_some(password);
        let verify = |user: &UserInfo| match &user.auth_info {
            AuthInfo::None => Ok(true),
            AuthInfo::Password {
                hash_value: h,
                hash_method: t,
                ..
            } => match &password {
                None => Err(ErrorCode::AuthenticateFailure("password required")),
                Some(p) => Ok(*h == t.hash(p)),
            },
            _ => Err(ErrorCode::AuthenticateFailure("wrong auth type")),
        };
        let (user, verified) = user_api
            .login_with_password(&tenant, identity, client_ip, &global_network_policy, verify)
            .await
            .map_err(|e| status!("login fail", e))?;
        match verified {
            Ok(true) => {}
            Ok(false) => return Err(Status::unauthenticated("wrong password")),
            Err(e) => return Err(Status::unauthenticated(e.message())),
        }

        session
            .set_authed_user(user, None)
            .await
            .map_err(|e| status!("set_authed_user fail {}", e))?;
        Ok(session)
    }
}

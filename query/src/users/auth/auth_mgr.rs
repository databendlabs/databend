use std::sync::Arc;

use common_arrow::arrow::compute::temporal::hour;
use common_clickhouse_srv::types::get_username_from_url;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::UserInfo;

pub use crate::configs::Config;
use crate::users::auth::jwt::JwtAuthenticator;
use crate::users::UserApiProvider;

pub struct AuthMgr {
    tenant: String,
    users: Arc<UserApiProvider>,
    jwt: Option<JwtAuthenticator>,
}

pub enum Credential {
    JWT {
        token: String,
    },
    Password {
        name: String,
        password: Vec<u8>,
        hostname: Option<String>,
    },
}

impl AuthMgr {
    pub async fn create(cfg: Config, users: Arc<UserApiProvider>) -> Result<Self> {
        Ok(AuthMgr {
            users,
            tenant: cfg.query.tenant_id.clone(),
            jwt: JwtAuthenticator::try_create(cfg).await?,
        })
    }

    pub fn auth(self, credential: &Credential) -> Result<UserInfo> {
        match credential {
            Credential::JWT { token: t } => {
                unimplemented!()
            }
            Credential::Password {
                name: n,
                password: p,
                hostname: h,
            } => {
                let user = self
                    .users
                    .get_user_with_client_ip(&self.tenant, name, &h.unwrap_or("%".to_string()))
                    .await?;
                match &user.auth_info {
                    AuthInfo::None => Ok(true),
                    AuthInfo::Password {
                        hash_value: h,
                        hash_method: t,
                    } => Ok(*h == t.hash(&p)),
                    _ => Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                }
                Ok(user)
            }
        }
    }
}

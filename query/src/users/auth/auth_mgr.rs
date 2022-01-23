use common_exception::Result;

pub use crate::configs::Config;
use crate::users::auth::jwt::JwtAuthenticator;

pub struct AuthMgr {
    jwt: Option<JwtAuthenticator>,
}

impl AuthMgr {
    pub async fn create(cfg: Config) -> Result<Self> {
        Ok(AuthMgr {
            jwt: JwtAuthenticator::try_create(cfg).await?,
        })
    }
}

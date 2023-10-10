use std::sync::Arc;

use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeType;

use crate::sessions::SessionContext;

#[async_trait::async_trait]
pub trait SessionPrivilegeManager {
    fn set_authed_user(&self, user: UserInfo, auth_role: Option<String>);

    fn set_current_role(&self, role: Option<RoleInfo>);

    fn get_current_role(&self) -> Result<RoleInfo>;

    fn get_available_roles(&self) -> Result<RoleInfo>;

    // fn show_grants(&self);

    async fn validate_privilege(object: &GrantObject, privilege: Vec<UserPrivilegeType>) -> Result<()>;

    async fn validate_owner(object: &GrantObject, user: &UserInfo) -> Result<()>;

    async fn validate_available_role(&self, role_name: &str) -> Result<()>;

    async fn check_visible(object: &GrantObject) -> Result<bool>;
}

pub struct SessionPrivilegeManagerImpl {
    session_ctx: Arc<SessionContext>,
}

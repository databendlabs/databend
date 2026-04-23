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

use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;

#[async_trait::async_trait]
pub trait TableContextAuthorization: Send + Sync {
    fn get_current_user(&self) -> Result<UserInfo>;

    fn get_current_role(&self) -> Option<RoleInfo>;

    fn get_secondary_roles(&self) -> Option<Vec<String>>;

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()>;

    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn get_visibility_checker(
        &self,
        ignore_ownership: bool,
        object: Object,
    ) -> Result<GrantObjectVisibilityChecker>;
}

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

use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::RoleIdentity;
use common_meta_types::RoleInfo;
use common_meta_types::SeqV;
use common_meta_types::UserPrivilegeSet;

#[async_trait::async_trait]
pub trait RoleApi: Sync + Send {
    async fn add_role(&self, role_info: &RoleInfo) -> Result<u64>;

    async fn get_role(&self, role: &RoleIdentity, seq: Option<u64>) -> Result<SeqV<RoleInfo>>;

    async fn get_roles(&self) -> Result<Vec<SeqV<RoleInfo>>>;

    async fn grant_role_privileges(
        &self,
        role: &RoleIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
        seq: Option<u64>,
    ) -> Result<Option<u64>>;

    async fn revoke_role_privileges(
        &self,
        role: &RoleIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
        seq: Option<u64>,
    ) -> Result<Option<u64>>;

    async fn drop_role(&self, role: &RoleIdentity, seq: Option<u64>) -> Result<()>;
}

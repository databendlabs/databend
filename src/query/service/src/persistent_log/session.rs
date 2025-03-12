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

use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;

pub fn get_persistent_log_user(tenant_id: &str, cluster_id: &str) -> UserInfo {
    let mut user = UserInfo::new_no_auth(
        format!("{}-{}-persistent-log", tenant_id, cluster_id).as_str(),
        "0.0.0.0",
    );
    user.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeType::CreateDatabase.into(),
    );
    user
}

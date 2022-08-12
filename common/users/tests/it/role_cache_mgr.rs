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

use std::collections::HashMap;
use std::collections::HashSet;

use common_base::base::tokio;
use common_exception::Result;
use common_grpc::RpcClientConf;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserPrivilegeSet;
use common_users::role_util::find_all_related_roles;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;

pub const CATALOG_DEFAULT: &str = "default";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_role_cache_mgr() -> Result<()> {
    let conf = RpcClientConf::default();
    UserApiProvider::init(conf).await?;
    RoleCacheManager::init()?;

    let mut role1 = RoleInfo::new("role1");
    role1.grants.grant_privileges(
        &GrantObject::Database(CATALOG_DEFAULT.to_owned(), "db1".to_string()),
        UserPrivilegeSet::available_privileges_on_database(),
    );
    UserApiProvider::instance().add_role("tenant1", role1, false).await?;

    let roles = RoleCacheManager::instance()
        .find_related_roles("tenant1", &["role1".to_string()])
        .await?;
    assert_eq!(roles.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_find_all_related_roles() -> Result<()> {
    let roles = vec![
        RoleInfo::new("role1"),
        RoleInfo::new("role2"),
        RoleInfo::new("role3"),
        RoleInfo::new("role4"),
        RoleInfo::new("role5"),
    ];
    // role1 -> role2 -> role4 -> role5
    //    <- -> role3
    let role_grants = vec![
        ("role1".to_string(), "role2".to_string()),
        ("role1".to_string(), "role3".to_string()),
        ("role2".to_string(), "role4".to_string()),
        ("role3".to_string(), "role1".to_string()),
        ("role4".to_string(), "role5".to_string()),
    ];
    let tests = vec![
        (vec!["role1".to_string()], vec![
            "role1", "role2", "role3", "role4", "role5",
        ]),
        (vec!["role3".to_string()], vec![
            "role1", "role2", "role3", "role4", "role5",
        ]),
    ];
    let mut cached: HashMap<String, RoleInfo> = roles
        .into_iter()
        .map(|r| (r.identity().to_string(), r))
        .collect();
    for (lhs, rhs) in role_grants {
        cached
            .get_mut(&lhs.to_string())
            .unwrap()
            .grants
            .grant_role(rhs)
    }
    for (input, want) in tests {
        let got: HashSet<_> = find_all_related_roles(&cached, &input)
            .into_iter()
            .map(|r| r.identity().to_string())
            .collect();
        let want: HashSet<_> = want.iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want);
    }
    Ok(())
}

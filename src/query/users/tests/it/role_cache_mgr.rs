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

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::role_util::find_all_related_roles;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;

pub const CATALOG_DEFAULT: &str = "default";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_role_cache_mgr() -> anyhow::Result<()> {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Init with default.
    {
        GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
    }
    let conf = RpcClientConf::empty();
    let tenant = Tenant::new_literal("tenant1");

    let user_manager = UserApiProvider::try_create_simple(conf, &tenant).await?;
    let role_cache_manager = RoleCacheManager::try_create(user_manager.clone())?;

    let mut role1 = RoleInfo::new("role1", None);
    role1.grants.grant_privileges(
        &GrantObject::Database(CATALOG_DEFAULT.to_owned(), "db1".to_string()),
        UserPrivilegeSet::available_privileges_on_database(false),
    );
    user_manager
        .add_role(&tenant, role1, &CreateOption::CreateOrReplace)
        .await?;

    let mut roles = role_cache_manager
        .find_related_roles(&tenant, &["role1".to_string()])
        .await?;
    roles.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(roles.len(), 2);
    assert_eq!(&roles[0].name, BUILTIN_ROLE_PUBLIC);
    assert_eq!(&roles[1].name, "role1");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_find_all_related_roles() -> anyhow::Result<()> {
    let roles = vec![
        RoleInfo::new("role1", None),
        RoleInfo::new("role2", None),
        RoleInfo::new("role3", None),
        RoleInfo::new("role4", None),
        RoleInfo::new("role5", None),
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

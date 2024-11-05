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

use std::collections::HashMap;

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_users::role_util::find_all_related_roles;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;

#[test]
fn test_find_all_related_roles() {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Init with default.
    {
        GlobalConfig::init(&InnerConfig::default()).unwrap();
    }

    // Create some test RoleInfo instances for the cache
    let mut role1 = RoleInfo::new("role1");
    let role2 = RoleInfo::new("role2");
    // Add more RoleInfo instances as needed for testing.

    // Create the cache HashMap and add the RoleInfo instances to it.
    let mut cache: HashMap<String, RoleInfo> = HashMap::new();
    cache.insert(role1.name.clone(), role1.clone());
    cache.insert(role2.name.clone(), role2.clone());
    // Add more RoleInfo instances to the cache as needed for testing.

    let role_identities = vec![BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()];

    // Call the find_all_related_roles function with the cache and role_identities.
    let result = find_all_related_roles(&cache, &role_identities);
    assert!(result.contains(&role1));
    assert!(result.contains(&role2));
    let role_identities = vec!["role1".to_string()];
    let result = find_all_related_roles(&cache, &role_identities);
    assert!(result.contains(&role1));
    assert!(!result.contains(&role2));
    let role_identities = vec!["role2".to_string()];
    let result = find_all_related_roles(&cache, &role_identities);
    assert!(!result.contains(&role1));
    assert!(result.contains(&role2));

    role1.grants.grant_role(role2.name.clone());
    // refresh cache
    cache.insert(role1.name.clone(), role1.clone());
    let role_identities = vec!["role1".to_string()];
    let result = find_all_related_roles(&cache, &role_identities);
    assert!(result.contains(&role1));
    assert!(result.contains(&role2));
    let role_identities = vec!["role2".to_string()];
    let result = find_all_related_roles(&cache, &role_identities);
    assert!(!result.contains(&role1));
    assert!(result.contains(&role2));
}

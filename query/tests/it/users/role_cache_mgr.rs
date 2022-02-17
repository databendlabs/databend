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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::RoleIdentity;
use common_meta_types::RoleInfo;
use databend_query::users::role_cache_mgr::find_all_related_roles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_find_all_related_roles() -> Result<()> {
    let roles = vec![
        RoleInfo::new("role1".to_string(), "%".to_string()),
        RoleInfo::new("role2".to_string(), "%".to_string()),
        RoleInfo::new("role3".to_string(), "%".to_string()),
        RoleInfo::new("role4".to_string(), "%".to_string()),
        RoleInfo::new("role5".to_string(), "%".to_string()),
    ];
    // role1 -> role2 -> role4 -> role5
    //    <- -> role3
    let role_grants = vec![
        (RoleIdentity::parse("role1"), RoleIdentity::parse("role2")),
        (RoleIdentity::parse("role1"), RoleIdentity::parse("role3")),
        (RoleIdentity::parse("role2"), RoleIdentity::parse("role4")),
        (RoleIdentity::parse("role3"), RoleIdentity::parse("role1")),
        (RoleIdentity::parse("role4"), RoleIdentity::parse("role5")),
    ];
    let tests = vec![
        (vec![RoleIdentity::parse("role1")], vec![
            "'role1'@'%'",
            "'role2'@'%'",
            "'role3'@'%'",
            "'role4'@'%'",
            "'role5'@'%'",
        ]),
        (vec![RoleIdentity::parse("role3")], vec![
            "'role1'@'%'",
            "'role2'@'%'",
            "'role3'@'%'",
            "'role4'@'%'",
            "'role5'@'%'",
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

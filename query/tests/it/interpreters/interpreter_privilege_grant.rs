// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PasswordHashMethod;
use common_meta_types::PrincipalIdentity;
use common_meta_types::RoleInfo;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use databend_query::interpreters::*;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grant_privilege_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    let mut planner = Planner::new(ctx.clone());

    let name = "test";
    let hostname = "localhost";
    let password = "test";
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(password),
        hash_method: PasswordHashMethod::Sha256,
    };

    let user_mgr = ctx.get_user_manager();
    user_mgr
        .add_user(&tenant, UserInfo::new(name, hostname, auth_info), false)
        .await?;
    user_mgr
        .add_role(&tenant, RoleInfo::new("role1"), false)
        .await?;

    struct Test {
        #[allow(dead_code)]
        name: &'static str,
        query: String,
        principal_identity: Option<PrincipalIdentity>,
        expected_grants: Option<UserGrantSet>,
        expected_err: Option<&'static str>,
    }

    let tests: Vec<Test> = vec![Test {
        name: "grant all on global",
        query: format!("GRANT ALL ON *.* TO '{}'@'{}'", name, hostname),
        principal_identity: Some(PrincipalIdentity::user(
            name.to_string(),
            hostname.to_string(),
        )),
        expected_grants: Some({
            let mut grants = UserGrantSet::empty();
            grants.grant_privileges(
                &GrantObject::Global,
                GrantObject::Global.available_privileges(),
            );
            grants
        }),
        expected_err: None,
    }];

    for tt in tests {
        let (plan, _, _) = planner.plan_sql(&tt.query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "GrantPrivilegeInterpreter");
        let r = match executor.execute().await {
            Err(err) => Err(err),
            Ok(mut stream) => {
                while let Some(_block) = stream.next().await {}
                Ok(())
            }
        };
        if tt.expected_err.is_some() {
            assert_eq!(
                tt.expected_err.unwrap(),
                r.unwrap_err().to_string(),
                "expected_err eq failed on query: {}",
                tt.query
            );
        } else {
            assert!(r.is_ok(), "got err on query {}: {:?}", tt.query, r);
        }
        if let Some(PrincipalIdentity::User(user)) = tt.principal_identity {
            let user_info = user_mgr.get_user(&tenant, user).await?;
            assert_eq!(user_info.grants, tt.expected_grants.unwrap())
        } else if let Some(PrincipalIdentity::Role(role)) = tt.principal_identity {
            let role_info = user_mgr.get_role(&tenant, role).await?;
            assert_eq!(role_info.grants, tt.expected_grants.unwrap())
        }
    }

    Ok(())
}

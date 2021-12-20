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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::PasswordType;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeSet;
use common_planners::*;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grant_privilege_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;
    let name = "test";
    let hostname = "localhost";
    let password = "test";
    let user_info = UserInfo::new(
        name.to_string(),
        hostname.to_string(),
        Vec::from(password),
        PasswordType::PlainText,
    );
    assert_eq!(user_info.grants, UserGrantSet::empty());
    let user_mgr = ctx.get_sessions_manager().get_user_manager();
    user_mgr.add_user(user_info).await?;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        query: String,
        user_identity: Option<(&'static str, &'static str)>,
        expected_grants: Option<UserGrantSet>,
        expected_err: Option<&'static str>,
    }

    let tests: Vec<Test> = vec![Test {
        name: "grant all",
        query: format!("GRANT ALL ON *.* TO '{}'@'{}'", name, hostname),
        user_identity: Some((name, hostname)),
        expected_grants: Some({
            let mut grants = UserGrantSet::empty();
            grants.grant_privileges(
                name,
                hostname,
                &GrantObject::Global,
                UserPrivilegeSet::all_privileges(),
            );
            grants
        }),
        expected_err: None,
    }];

    for tt in tests {
        if let PlanNode::GrantPrivilege(plan) = PlanParser::parse(&tt.query, ctx.clone()).await? {
            let executor = GrantPrivilegeInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(executor.name(), "GrantPrivilegeInterpreter");
            match executor.execute(None).await {
                Err(err) => {
                    if tt.expected_err.is_some() {
                        assert_eq!(tt.expected_err.unwrap(), err.to_string());
                    } else {
                        return Err(err);
                    }
                }
                Ok(mut stream) => while let Some(_block) = stream.next().await {},
            };
            if let Some((name, hostname)) = tt.user_identity {
                let new_user = user_mgr.get_user(name, hostname).await?;
                assert_eq!(new_user.grants, tt.expected_grants.unwrap())
            }
        } else {
            panic!()
        }
    }

    Ok(())
}

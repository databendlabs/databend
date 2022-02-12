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
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeType;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grant_privilege_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;
    let tenant = ctx.get_tenant();

    let name = "test";
    let hostname = "localhost";
    let password = "test";
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(password),
        hash_method: PasswordHashMethod::PlainText,
    };
    let user_info = UserInfo::new(name.to_string(), hostname.to_string(), auth_info);
    assert_eq!(user_info.grants, UserGrantSet::empty());
    let user_mgr = ctx.get_user_manager();
    user_mgr.add_user(&tenant, user_info).await?;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        query: String,
        user_identity: Option<(&'static str, &'static str)>,
        expected_grants: Option<UserGrantSet>,
        expected_err: Option<&'static str>,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "grant create user to global",
            query: format!("GRANT CREATE USER ON *.* TO '{}'@'{}'", name, hostname),
            user_identity: Some((name, hostname)),
            expected_grants: Some({
                let mut grants = UserGrantSet::empty();
                grants.grant_privileges(
                    name,
                    hostname,
                    &GrantObject::Global,
                    vec![UserPrivilegeType::CreateUser].into(),
                );
                grants
            }),
            expected_err: None,
        },
        Test {
            name: "grant create user to current database and expect err",
            query: format!("GRANT CREATE USER ON * TO '{}'@'{}'", name, hostname),
            user_identity: None,
            expected_grants: None,
            expected_err: Some("Code: 1061, displayText = Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used."),
        },
        Test {
            name: "grant all on global",
            query: format!("GRANT ALL ON *.* TO '{}'@'{}'", name, hostname),
            user_identity: Some((name, hostname)),
            expected_grants: Some({
                let mut grants = UserGrantSet::empty();
                grants.grant_privileges(
                    name,
                    hostname,
                    &GrantObject::Global,
                    GrantObject::Global.available_privileges(),
                );
                grants
            }),
            expected_err: None,
        },
    ];

    for tt in tests {
        let plan = PlanParser::parse(ctx.clone(), &tt.query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "GrantPrivilegeInterpreter");
        let r = match executor.execute(None).await {
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
        if let Some((name, hostname)) = tt.user_identity {
            let new_user = user_mgr.get_user(&tenant, name, hostname).await?;
            assert_eq!(new_user.grants, tt.expected_grants.unwrap())
        }
    }

    Ok(())
}

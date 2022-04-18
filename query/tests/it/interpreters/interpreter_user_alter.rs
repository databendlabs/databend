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
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserInfo;
use common_meta_types::UserOptionFlag;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alter_user_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;
    let tenant = "test";
    let name = "test";
    let hostname = "localhost";
    let password = "test";
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(password),
        hash_method: PasswordHashMethod::PlainText,
    };

    let user_info = UserInfo::new(name, hostname, auth_info);
    let user_mgr = ctx.get_user_manager();
    user_mgr.add_user(tenant, user_info.clone(), false).await?;

    let old_user = user_mgr.get_user(tenant, user_info.identity()).await?;
    assert_eq!(
        old_user.auth_info.get_password().unwrap(),
        Vec::from(password)
    );

    {
        let new_password = "password";
        let test_query = format!(
            "ALTER USER '{}'@'{}' IDENTIFIED BY '{}'",
            name, hostname, new_password
        );

        let plan = PlanParser::parse(ctx.clone(), &test_query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "AlterUserInterpreter");
        let mut stream = executor.execute(None).await?;
        while let Some(_block) = stream.next().await {}
        let new_user = user_mgr.get_user(tenant, user_info.identity()).await?;
        assert_eq!(
            new_user.auth_info.get_password(),
            Some(Vec::from(new_password))
        );
    }

    {
        let new_password = "new_password";
        let test_query = format!(
            "ALTER USER '{}'@'{}' WITH TENANTSETTING IDENTIFIED BY '{}'",
            name, hostname, new_password
        );
        let plan = PlanParser::parse(ctx.clone(), &test_query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "AlterUserInterpreter");
        executor.execute(None).await?;
        let user_info = user_mgr.get_user(tenant, user_info.identity()).await?;
        assert!(user_info.has_option_flag(UserOptionFlag::TenantSetting));
        assert_eq!(
            user_info.auth_info.get_password(),
            Some(Vec::from(new_password))
        );
    }

    {
        let test_query = format!("ALTER USER '{}'@'{}' WITH NOTENANTSETTING", name, hostname);
        let plan = PlanParser::parse(ctx.clone(), &test_query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "AlterUserInterpreter");
        executor.execute(None).await?;
        let user_info = user_mgr.get_user(tenant, user_info.identity()).await?;
        assert!(!user_info.has_option_flag(UserOptionFlag::TenantSetting));
    }

    Ok(())
}

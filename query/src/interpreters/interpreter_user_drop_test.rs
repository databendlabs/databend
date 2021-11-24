// Copyright 2020 Datafuse Labs.
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
use common_meta_types::AuthType;
use common_meta_types::UserInfo;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_user_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::try_create_context()?;

    {
        static TEST_QUERY: &str = "DROP USER 'test'@'localhost'";
        if let PlanNode::DropUser(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = DropUserInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(executor.name(), "DropUserInterpreter");
            let ret = executor.execute(None).await;
            assert!(ret.is_err())
        } else {
            panic!()
        }
    }

    {
        static TEST_QUERY: &str = "DROP USER IF EXISTS 'test'@'localhost'";
        if let PlanNode::DropUser(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = DropUserInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(executor.name(), "DropUserInterpreter");
            let ret = executor.execute(None).await;
            assert!(ret.is_ok())
        } else {
            panic!()
        }
    }

    {
        let name = "test";
        let hostname = "localhost";
        let password = "test";
        let user_info = UserInfo::new(
            name.to_string(),
            hostname.to_string(),
            Vec::from(password),
            AuthType::PlainText,
        );
        let user_mgr = ctx.get_sessions_manager().get_user_manager();
        user_mgr.add_user(user_info).await?;

        let old_user = user_mgr.get_user(name, hostname).await?;
        assert_eq!(old_user.password, Vec::from(password));

        static TEST_QUERY: &str = "DROP USER 'test'@'localhost'";
        if let PlanNode::DropUser(plan) = PlanParser::parse(TEST_QUERY, ctx.clone()).await? {
            let executor = DropUserInterpreter::try_create(ctx, plan.clone())?;
            assert_eq!(executor.name(), "DropUserInterpreter");
            executor.execute(None).await?;
        } else {
            panic!()
        }
    }

    Ok(())
}

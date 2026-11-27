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

use std::str;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::UserApiProvider;
use futures::TryStreamExt;

use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContextSettings;
use crate::sql::Planner;

/// If you no need to care the ctx please use TestFixture.execute_query().
pub async fn execute_query(ctx: Arc<QueryContext>, query: &str) -> Result<SendableDataBlockStream> {
    // A reused context must not leak runtime filter state from a previous
    // plan into this one: scan ids restart per plan.
    ctx.clear_runtime_filter();
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    executor.execute(ctx.clone()).await
}

/// If you no need to care the ctx please use TestFixture.execute_command().
pub async fn execute_command(ctx: Arc<QueryContext>, query: &str) -> Result<()> {
    let res = execute_query(ctx, query).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

/// Stores `user` in meta-service and authenticates `session` as it. A MySQL or Flight SQL
/// session reloads its user from meta-service before each query, see `Session::reload_authed_user`.
pub async fn set_meta_user(session: &Session, mut user: UserInfo) -> Result<()> {
    let user_api = UserApiProvider::instance();
    let tenant = session.get_current_tenant();
    // Replacing the user stands for altering it: sessions logged in as it before stay valid.
    if let Ok(stored) = user_api.get_user(&tenant, user.identity()).await {
        user.created_on = stored.created_on;
    }
    user_api
        .create_user(&tenant, user.clone(), &CreateOption::CreateOrReplace)
        .await?;
    session.set_authed_user(user, None).await
}

pub async fn execute_pipeline(ctx: Arc<QueryContext>, mut res: PipelineBuildResult) -> Result<()> {
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let mut pipelines = res.sources_pipelines;
    pipelines.push(res.main_pipeline);
    let executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute().await
}

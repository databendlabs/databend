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

use std::sync::Arc;

use common_exception::Result;
use common_expression::types::number::UInt64Type;
use common_expression::types::BooleanType;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_sql::plans::ShowRolesPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct ShowRolesInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowRolesPlan,
}

impl ShowRolesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowRolesPlan) -> Result<Self> {
        Ok(ShowRolesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowRolesInterpreter {
    fn name(&self) -> &str {
        "ShowRolesInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let session = self.ctx.get_current_session();
        let mut roles = session.get_all_available_roles().await?;
        roles.sort_by(|a, b| a.name.cmp(&b.name));

        let current_role_name = session
            .get_current_role()
            .map(|r| r.name)
            .unwrap_or_default();
        let default_role_name = session
            .get_current_user()?
            .option
            .default_role()
            .cloned()
            .unwrap_or_default();

        let names = roles
            .iter()
            .map(|x| x.name.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let inherited_roles: Vec<u64> = roles
            .iter()
            .map(|x| x.grants.roles().len() as u64)
            .collect();
        let is_currents: Vec<bool> = roles.iter().map(|r| r.name == current_role_name).collect();
        let is_defaults: Vec<bool> = roles.iter().map(|r| r.name == default_role_name).collect();

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(inherited_roles),
            BooleanType::from_data(is_currents),
            BooleanType::from_data(is_defaults),
        ])])
    }
}

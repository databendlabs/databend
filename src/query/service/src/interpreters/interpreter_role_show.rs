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

use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_storages_fuse::TableContext;
use itertools::Itertools;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowRolesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowRolesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowRolesInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowRolesInterpreter {
    fn name(&self) -> &str {
        "ShowRolesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "show_roles_execute");

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

        let names = roles.iter().map(|x| x.name.clone()).collect::<Vec<_>>();
        let inherited_roles: Vec<u64> = roles
            .iter()
            .map(|x| x.grants.roles().len() as u64)
            .collect();
        let inherited_roles_names: Vec<String> = roles
            .iter()
            .map(|x| x.grants.roles().iter().sorted().join(", ").to_string())
            .collect();
        let is_currents: Vec<bool> = roles.iter().map(|r| r.name == current_role_name).collect();
        let is_defaults: Vec<bool> = roles.iter().map(|r| r.name == default_role_name).collect();

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(inherited_roles),
            StringType::from_data(inherited_roles_names),
            BooleanType::from_data(is_currents),
            BooleanType::from_data(is_defaults),
        ])])
    }
}

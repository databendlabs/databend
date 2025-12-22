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

use databend_common_ast::ast::Connection;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct ShowConnectionsInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowConnectionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowConnectionsInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowConnectionsInterpreter {
    fn name(&self) -> &str {
        "ShowConnectionsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "show_connections_execute");

        let user_mgr = UserApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let mut formats = user_mgr.get_connections(&tenant).await?;

        formats.sort_by(|a, b| a.name.cmp(&b.name));

        if self
            .ctx
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            let visibility_checker = self
                .ctx
                .get_visibility_checker(false, Object::Connection)
                .await?;
            formats.retain(|c| visibility_checker.check_connection_visibility(&c.name));
        }

        // Merge three independent 'map().collect()' into one iteration.
        let capacity = formats.len();
        let mut names = Vec::with_capacity(capacity);
        let mut types = Vec::with_capacity(capacity);
        let mut options = Vec::with_capacity(capacity);

        for c in formats.iter_mut() {
            names.push(c.name.clone());
            types.push(c.storage_type.clone());

            let conn = Connection::new(c.storage_params.clone()).mask();
            c.storage_params = conn.conns;
            options.push(c.storage_params_display().clone());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            StringType::from_data(options),
        ])])
    }
}

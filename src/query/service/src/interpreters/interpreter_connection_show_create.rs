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

use std::fmt::Write;
use std::sync::Arc;

use databend_common_ast::ast::Connection;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_sql::plans::ShowCreateConnectionPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct ShowCreateConnectionInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateConnectionPlan,
}

impl ShowCreateConnectionInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateConnectionPlan) -> Result<Self> {
        Ok(ShowCreateConnectionInterpreter { ctx, plan })
    }

    /// Credentials are masked, so the statement does not recreate a
    /// connection that holds any.
    fn show_create_connection(connection: &UserDefinedConnection) -> String {
        // The name is kept as written in `CREATE CONNECTION`, quotes included.
        let mut sql = format!(
            "CREATE CONNECTION {} STORAGE_TYPE = {}",
            connection.name,
            QuotedString(&connection.storage_type, '\'')
        );
        let params = Connection::new(connection.storage_params.clone()).mask_secrets();
        for (k, v) in &params.conns {
            write!(sql, " {} = {}", k, QuotedString(v, '\'')).expect("write to string");
        }
        sql
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateConnectionInterpreter {
    fn name(&self) -> &str {
        "ShowCreateConnectionInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let tenant = self.ctx.get_tenant();
            let connection = UserApiProvider::instance()
                .get_connection(&tenant, self.plan.name.as_str())
                .await?;
            let sql = Self::show_create_connection(&connection);

            PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                StringType::from_data(vec![connection.name]),
                StringType::from_data(vec![sql]),
            ])])
        })
    }
}

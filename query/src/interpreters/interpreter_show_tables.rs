// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::PlanShowKind;
use common_planners::ShowTablesPlan;
use common_streams::SendableDataBlockStream;

use crate::catalogs::DatabaseCatalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

pub struct ShowTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowTablesPlan,
}

impl ShowTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowTablesPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowTablesInterpreter { ctx, plan }))
    }

    fn build_query(&self) -> Result<String> {
        let mut database = self.ctx.get_current_database();
        if let Some(v) = &self.plan.fromdb {
            database = v.to_string();
        }

        if DatabaseCatalog::is_case_insensitive_db(&database) {
            database = database.to_uppercase()
        }

        let showfull = self.plan.showfull;
        let select_cols = if showfull {
            format!(
                "table_name as Tables_in_{}, table_type as Table_type",
                database
            )
        } else {
            format!("table_name as Tables_in_{}", database)
        };

        match &self.plan.kind {
            PlanShowKind::All => {
                Ok(format!("SELECT {} FROM information_schema.tables WHERE table_schema = '{}' ORDER BY table_schema, table_name", select_cols, database))
            }
            PlanShowKind::Like(v) => {
                Ok(format!("SELECT {} FROM information_schema.tables WHERE table_schema = '{}' AND table_name LIKE {} ORDER BY table_schema, table_name", select_cols, database, v))
            }
            PlanShowKind::Where(v) => {
                Ok(format!("SELECT {} FROM information_schema.tables WHERE table_schema = '{}' AND ({}) ORDER BY table_schema, table_name", select_cols, database, v))
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowTablesInterpreter {
    fn name(&self) -> &str {
        "ShowTablesInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let query = self.build_query()?;
        let plan = PlanParser::parse(self.ctx.clone(), &query).await?;
        let optimized = Optimizers::create(self.ctx.clone()).optimize(&plan)?;

        if let PlanNode::Select(plan) = optimized {
            let interpreter = SelectInterpreter::try_create(self.ctx.clone(), plan)?;
            interpreter.execute(input_stream).await
        } else {
            return Err(ErrorCode::LogicalError("Show tables build query error"));
        }
    }
}

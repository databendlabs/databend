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

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::TryStreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;
use crate::sql::Planner;
use crate::storages::system::TablesTableWithoutHistory;

pub struct SearchTablesProcedure {}

impl SearchTablesProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(SearchTablesProcedure {}.into_procedure())
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for SearchTablesProcedure {
    fn name(&self) -> &str {
        "SEARCH_TABLES"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .num_arguments(1)
            .management_mode_required(true)
    }

    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let query = format!(
            "SELECT * FROM system.tables WHERE name like '%{}%' ORDER BY database, name",
            args[0]
        );
        let mut planner = Planner::new(ctx.clone());
        let (plan, _, _) = planner.plan_sql(&query).await?;

        let stream = if let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        {
            let interpreter =
                SelectInterpreterV2::try_create(ctx.clone(), *bind_context, *s_expr, metadata)?;
            interpreter.execute(ctx.clone()).await
        } else {
            return Err(ErrorCode::LogicalError("search tables build query error"));
        }?;
        let result = stream.try_collect::<Vec<_>>().await?;
        if !result.is_empty() {
            Ok(result[0].clone())
        } else {
            Ok(DataBlock::empty())
        }
    }

    fn schema(&self) -> Arc<DataSchema> {
        TablesTableWithoutHistory::schema()
    }
}

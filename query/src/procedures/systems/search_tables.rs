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
use common_planners::PlanNode;
use futures::TryStreamExt;

use crate::interpreters::SelectInterpreter;
use crate::optimizers::Optimizers;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;
use crate::storages::system::TablesTable;

pub struct SearchTablesProcedure {}

impl SearchTablesProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(SearchTablesProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for SearchTablesProcedure {
    fn name(&self) -> &str {
        "SEARCH_TABLES"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .num_arguments(1)
            .management_mode_required(true)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let query = format!(
            "SELECT * FROM system.tables WHERE name like '%{}%' ORDER BY database, name",
            args[0]
        );
        let plan = PlanParser::parse(ctx.clone(), &query).await?;
        let optimized = Optimizers::create(ctx.clone()).optimize(&plan)?;

        let stream = if let PlanNode::Select(plan) = optimized {
            let interpreter = SelectInterpreter::try_create(ctx.clone(), plan)?;
            interpreter.execute(None).await
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
        TablesTable::schema()
    }
}

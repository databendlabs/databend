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
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_planners::ExistsTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ExistsTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ExistsTablePlan,
}

impl ExistsTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExistsTablePlan) -> Result<Self> {
        Ok(ExistsTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ExistsTableInterpreter {
    fn name(&self) -> &str {
        "ExistsTableInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.plan.catalog.as_str();
        let database = self.plan.database.as_str();
        let table = self.plan.table.as_str();
        let exists = self.ctx.get_table(catalog, database, table).await.is_ok();
        let result = match exists {
            true => 1u8,
            false => 0u8,
        };

        PipelineBuildResult::from_blocks(vec![
            DataBlock::create(
                self.plan.schema(),
                vec![Series::from_data(vec![result])],
            )
        ])
    }
}

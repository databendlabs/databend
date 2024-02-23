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

use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::AnalyzeTablePlan;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use itertools::Itertools;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AnalyzeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: AnalyzeTablePlan,
}

impl AnalyzeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AnalyzeTablePlan) -> Result<Self> {
        Ok(AnalyzeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AnalyzeTableInterpreter {
    fn name(&self) -> &str {
        "AnalyzeTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        // check mutability
        table.check_mutable()?;

        // Only fuse table can apply analyze
        let table = match FuseTable::try_from_table(table.as_ref()) {
            Ok(t) => t,
            Err(_) => return Ok(PipelineBuildResult::create()),
        };

        let r = table.read_table_snapshot().await;
        let snapshot_opt = match r {
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        if let Some(_snapshot) = snapshot_opt {
            // plan sql
            let schema = table.schema();
            let _table_info = table.get_table_info();
            let index_cols: Vec<(u32, String)> = schema
                .fields()
                .iter()
                .filter(|f| RangeIndex::supported_type(&f.data_type().into()))
                .map(|f| (f.column_id(), f.name.clone()))
                .collect();
            let select_expr = index_cols
                .iter()
                .map(|c| format!("approx_count_distinct({}) as ndv_{}", c.1, c.0))
                .join(", ");

            let _sql = format!("SELECT {select_expr} from {}.{}", plan.database, plan.table);

            todo!()
        }

        return Ok(PipelineBuildResult::create());
    }
}

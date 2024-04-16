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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_sql::plans::DescribeViewPlan;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::util::generate_desc_schema;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sql::Planner;

pub struct DescribeViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeViewPlan,
}

impl DescribeViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescribeViewPlan) -> Result<Self> {
        Ok(DescribeViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeViewInterpreter {
    fn name(&self) -> &str {
        "DescribeViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.plan.catalog.as_str();
        let database = self.plan.database.as_str();
        let view = self.plan.view_name.as_str();
        let table = self.ctx.get_table(catalog, database, view).await?;
        let tbl_info = table.get_table_info();
        let engine = table.get_table_info().engine();
        let schema = if engine == VIEW_ENGINE {
            if let Some(query) = tbl_info.options().get(QUERY) {
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(query).await?;
                infer_table_schema(&plan.schema())
            } else {
                return Err(ErrorCode::Internal(
                    "Logical error, View Table must have a SelectQuery inside.",
                ));
            }
        } else {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not VIEW, please use `DESC {} {}.{}`",
                &self.plan.database,
                &self.plan.view_name,
                if engine == STREAM_ENGINE {
                    "STREAM"
                } else {
                    "TABLE"
                },
                &self.plan.database,
                &self.plan.view_name
            )));
        }?;

        let (names, types, nulls, default_exprs, extras) = generate_desc_schema(schema);

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            StringType::from_data(nulls),
            StringType::from_data(default_exprs),
            StringType::from_data(extras),
        ])])
    }
}

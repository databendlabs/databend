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

use std::collections::HashSet;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ROW_ID_COL_NAME;
use common_sql::executor::MergeIntoSource;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::MergeInto;

use super::Interpreter;
use super::InterpreterPtr;
use super::SelectInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct MergeIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: MergeInto,
}

impl MergeIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: MergeInto) -> Result<InterpreterPtr> {
        Ok(Arc::new(MergeIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for MergeIntoInterpreter {
    fn name(&self) -> &str {
        "MergeIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        todo!()
    }
}

impl MergeIntoInterpreter {
    async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let mut row_id_idx = -1;
        let MergeInto {
            bind_context,
            input,
            meta_data,
            columns_set,
            ..
        } = &self.plan;

        let mut builder = PhysicalPlanBuilder::new(meta_data.clone(), self.ctx.clone(), false);

        // build source for MergeInto
        let join_input = builder.build(&**input, *columns_set.clone()).await?;

        // find row_id column index
        let join_output_schema = join_input.output_schema()?;

        for (idx, data_filed) in join_output_schema.fields().into_iter().enumerate() {
            if data_filed.name() == ROW_ID_COL_NAME {
                row_id_idx = idx as i32;
                break;
            }
        }

        // we can't get row_id_idx, throw a exception
        if row_id_idx == -1 {
            return Err(ErrorCode::InValidRowIdIndex(
                "can't get internal row_id_idx when running merge into",
            ));
        }

        let merge_into_source = PhysicalPlan::MergeIntoSource(MergeIntoSource {
            input: Box::new(join_input),
            row_id_idx: row_id_idx as u32,
            row_id_set: HashSet::new(),
        });

        todo!()
    }
}

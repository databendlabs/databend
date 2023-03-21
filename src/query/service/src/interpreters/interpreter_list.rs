// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_expression::types::number::UInt64Type;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_expression::FromOptData;
use common_sql::plans::ListPlan;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_stage::StageTable;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ListInterpreter {
    ctx: Arc<QueryContext>,
    plan: ListPlan,
}

impl ListInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ListPlan) -> Result<Self> {
        Ok(ListInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ListInterpreter {
    fn name(&self) -> &str {
        "ListInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "list_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;

        let op = StageTable::get_op(&plan.stage)?;

        let pattern = if plan.pattern.is_empty() {
            None
        } else {
            Some(plan.pattern.clone())
        };
        let files_info = StageFilesInfo {
            path: plan.path.clone(),
            files: None,
            pattern,
        };
        let files: Vec<StageFileInfo> = files_info.list(&op, false).await?;

        let names: Vec<Vec<u8>> = files
            .iter()
            .map(|file| file.path.to_string().into_bytes())
            .collect();
        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        let etags: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.etag.as_ref().map(|f| f.to_string().into_bytes()))
            .collect();
        let last_modifieds: Vec<Vec<u8>> = files
            .iter()
            .map(|file| {
                file.last_modified
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
                    .into_bytes()
            })
            .collect();
        let creators: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.creator.as_ref().map(|c| c.to_string().into_bytes()))
            .collect();

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(sizes),
            StringType::from_opt_data(etags),
            StringType::from_data(last_modifieds),
            StringType::from_opt_data(creators),
        ])])
    }
}

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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::ListPlan;
use common_storages_stage::list_file;
use regex::Regex;

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
        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        let mut files = list_file(table_ctx, &plan.path, &plan.stage).await?;

        let files = if plan.pattern.is_empty() {
            files
        } else {
            let regex = Regex::new(&plan.pattern).map_err(|e| {
                ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    &plan.pattern, e
                ))
            })?;
            files.retain(|v| regex.is_match(&v.path));
            files
        };

        let names: Vec<String> = files.iter().map(|file| file.path.clone()).collect();
        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        let etags: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.etag.as_ref().map(|f| f.to_string().into_bytes()))
            .collect();
        let last_modifieds: Vec<String> = files
            .iter()
            .map(|file| {
                file.last_modified
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
            })
            .collect();
        let creators: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.creator.as_ref().map(|c| c.to_string().into_bytes()))
            .collect();

        PipelineBuildResult::from_blocks(vec![DataBlock::create(self.plan.schema(), vec![
            Series::from_data(names),
            Series::from_data(sizes),
            Series::from_data(etags),
            Series::from_data(last_modifieds),
            Series::from_data(creators),
        ])])
    }
}

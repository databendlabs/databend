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
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_planners::ListPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::interpreter_common::list_files;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct ListInterpreter {
    ctx: Arc<QueryContext>,
    plan: ListPlan,
}

impl ListInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ListPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ListInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ListInterpreter {
    fn name(&self) -> &str {
        "ListInterpreter"
    }

    #[tracing::instrument(level = "debug", name = "list_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let files = list_files(&self.ctx, &plan.stage, &plan.path, &plan.pattern).await?;

        let names: Vec<String> = files.iter().map(|file| file.path.clone()).collect();
        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        let md5s: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.md5.as_ref().map(|f| f.to_string().into_bytes()))
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

        let block = DataBlock::create(self.plan.schema(), vec![
            Series::from_data(names),
            Series::from_data(sizes),
            Series::from_data(md5s),
            Series::from_data(last_modifieds),
            Series::from_data(creators),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![block],
        )))
    }
}

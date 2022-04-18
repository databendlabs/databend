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

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::get_file_name;
use common_io::prelude::S3File;
use common_planners::ListPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use regex::Regex;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::StageSource;

pub struct ListInterpreter {
    ctx: Arc<QueryContext>,
    plan: ListPlan,
}

impl ListInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ListPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ListInterpreter { ctx, plan }))
    }

    async fn list_files(&self) -> Result<Vec<String>> {
        let op = StageSource::get_op(&self.ctx, &self.plan.stage).await?;
        let pattern = &self.plan.pattern;
        let path = &self.plan.path;
        let mut files = S3File::list(&op, path).await?;

        if !pattern.is_empty() {
            let regex = Regex::new(pattern).map_err(|e| {
                ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    pattern, e
                ))
            })?;

            let matched_files = files
                .iter()
                .filter(|file| regex.is_match(file))
                .cloned()
                .collect();
            files = matched_files;
        }

        Ok(files)
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
        let files = self.list_files().await?;
        tracing::info!("list file list:{:?}, pattern:{}", &files, self.plan.pattern);

        // file path to filename
        let files: Vec<String> = files.iter().map(|file| get_file_name(file)).collect();

        let block = DataBlock::create(self.plan.schema(), vec![Series::from_data(files)]);
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![block],
        )))
    }
}

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

use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_sql::plans::ShowFileFormatsPlan;
use common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct ShowFileFormatsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowFileFormatsPlan,
}

impl ShowFileFormatsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowFileFormatsPlan) -> Result<Self> {
        Ok(ShowFileFormatsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowFileFormatsInterpreter {
    fn name(&self) -> &str {
        "ShowFileFormatsInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let user_mgr = UserApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let mut formats = user_mgr.get_file_formats(&tenant).await?;

        formats.sort_by(|a, b| a.name.cmp(&b.name));

        let names = formats
            .iter()
            .map(|x| x.name.as_bytes().to_vec())
            .collect::<Vec<_>>();

        let options = formats
            .iter()
            .map(|x| x.file_format_params.to_string().as_bytes().to_vec())
            .collect::<Vec<_>>();

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(options),
        ])])
    }
}

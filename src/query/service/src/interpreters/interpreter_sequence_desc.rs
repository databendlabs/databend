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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_sql::plans::DescSequencePlan;
use databend_common_storages_fuse::TableContext;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct DescSequenceInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescSequencePlan,
}

impl DescSequenceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescSequencePlan) -> Result<Self> {
        Ok(DescSequenceInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescSequenceInterpreter {
    fn name(&self) -> &str {
        "DescSequenceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let req = GetSequenceReq {
            ident: self.plan.ident.clone(),
        };
        let catalog = self.ctx.get_default_catalog()?;
        // Already check seq privilege before interpreter
        let reply = catalog.get_sequence(req, &None).await?;

        let name = vec![self.plan.ident.name().to_string()];
        let interval = vec![reply.meta.step];
        let current = vec![reply.meta.current];
        let created_on = vec![reply.meta.create_on.timestamp_micros()];
        let updated_on = vec![reply.meta.update_on.timestamp_micros()];
        let comment = vec![reply.meta.comment];
        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(name),
            Int64Type::from_data(interval),
            UInt64Type::from_data(current),
            TimestampType::from_data(created_on),
            TimestampType::from_data(updated_on),
            StringType::from_opt_data(comment),
        ])];
        PipelineBuildResult::from_blocks(blocks)
    }
}

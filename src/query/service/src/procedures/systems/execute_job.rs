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

use background_service::get_background_service_handler;
pub use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

pub struct ExecuteJobProcedure {}

impl ExecuteJobProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(ExecuteJobProcedure {}.into_procedure())
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for ExecuteJobProcedure {
    fn name(&self) -> &str {
        "EXECUTE_JOB"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .num_arguments(1)
            .management_mode_required(false)
    }

    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let name = args[0].clone();
        let background_handler = get_background_service_handler();
        background_handler.execute_scheduled_job(ctx, name).await?;
        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![])
    }
}

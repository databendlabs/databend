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
use common_catalog::table_context::TableContext;
pub use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_procedures::ProcedureFeatures;
use common_procedures::ProcedureSignature;

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::sessions::QueryContext;

pub struct ExecuteJobProcedure {
    sig: Box<dyn ProcedureSignature>,
}

impl ExecuteJobProcedure {
    pub fn try_create(sig: Box<dyn ProcedureSignature>) -> Result<Box<dyn Procedure>> {
        Ok(ExecuteJobProcedure { sig }.into_procedure())
    }
}

impl ProcedureSignature for ExecuteJobProcedure {
    fn name(&self) -> &str {
        self.sig.name()
    }

    fn features(&self) -> ProcedureFeatures {
        self.sig.features()
    }

    fn schema(&self) -> Arc<DataSchema> {
        self.sig.schema()
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for ExecuteJobProcedure {
    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        assert_eq!(args.len(), 1);
        let name = args[0].clone();
        let background_handler = get_background_service_handler();

        background_handler
            .execute_scheduled_job(ctx.get_tenant(), ctx.get_current_user()?.identity(), name)
            .await?;
        Ok(DataBlock::empty())
    }
}

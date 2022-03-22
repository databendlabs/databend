//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::Result;

use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

pub struct ReloadConfigProcedure {}

impl ReloadConfigProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(ReloadConfigProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for ReloadConfigProcedure {
    fn name(&self) -> &str {
        "RELOAD_CONFIG"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, _: Vec<String>) -> Result<DataBlock> {
        // TODO: check permissions
        ctx.reload_config().await?;
        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::empty())
    }
}

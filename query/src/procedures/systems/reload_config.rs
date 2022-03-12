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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::catalogs::Catalog;
use crate::procedures::Procedure;
use crate::procedures::ProcedureDescription;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::storages::fuse::FuseHistory;
use crate::storages::fuse::FuseTable;

pub struct ReloadConfigProcedure {}

impl ReloadConfigProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(ReloadConfigProcedure {}))
    }

    pub fn desc() -> ProcedureDescription {
        ProcedureDescription::creator(Box::new(Self::try_create))
            .features(ProcedureFeatures::default())
    }
}

#[async_trait::async_trait]
impl Procedure for ReloadConfigProcedure {
    async fn eval(&self, ctx: Arc<QueryContext>, _: Vec<String>) -> Result<DataBlock> {
        // TODO: check permissions
        ctx.reload_config()?;
        Ok(DataBlock::empty())
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::empty())
    }
}

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
use common_datavalues::{DataField, DataSchema, Series, SeriesFrom, StringColumn, StringType};
use common_exception::{ErrorCode, Result};
use common_meta_types::UserOptionFlag;
use common_tracing::tracing;

use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::{QueryContext, SessionRef};

pub struct ProfilingQueryProcedure {}

impl ProfilingQueryProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(ProfilingQueryProcedure {}))
    }

    fn profiling_query(&self, session: SessionRef) -> Result<DataBlock> {
        let profiling_info = session.profiling_query()?;
        match serde_json::to_string(&profiling_info) {
            Ok(json_str) => Ok(DataBlock::create(self.schema(), vec![Series::from_data(vec![json_str])])),
            Err(cause) => Err(ErrorCode::LogicalError(format!("Cannot format profiling info, cause {:?}", cause))),
        }
    }
}

#[async_trait::async_trait]
impl Procedure for ProfilingQueryProcedure {
    fn name(&self) -> &str {
        "RELOAD_CONFIG"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().num_arguments(1)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        // TODO: check permissions
        let query_id = &args[0];
        tracing::info!("ProfilingQuery: query_id: {:?}", query_id);
        match ctx.get_session_by_id(query_id).await {
            Some(session) => self.profiling_query(session),
            None => Err(ErrorCode::NotFoundSession(format!("Not found session by id: {}", query_id))),
        }
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(DataSchema::new(vec![DataField::new("profiling", StringType::arc())]))
    }
}

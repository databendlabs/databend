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

pub struct ClusteringInformationProcedure {}

impl ClusteringInformationProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(ClusteringInformationProcedure {}))
    }
}


#[async_trait::async_trait]
impl Procedure for ClusteringInformationProcedure {
    fn name(&self) -> &str {
        "CLUSTERING_INFORMATION"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().variadic_arguments(1,2)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        todo!()
    }

    fn schema(&self) -> Arc<DataSchema> {
        todo!()
    }
}

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
use common_expression::DataSchema;
use common_storages_fuse::table_functions::ClusteringInformation;

use crate::ProcedureFeatures;
use crate::ProcedureSignature;

pub struct ClusteringInformationProcedureSig {}

impl ClusteringInformationProcedureSig {
    pub fn try_create() -> Result<Box<dyn ProcedureSignature>> {
        Ok(Box::new(ClusteringInformationProcedureSig {}))
    }
}

impl ProcedureSignature for ClusteringInformationProcedureSig {
    fn name(&self) -> &str {
        "CLUSTERING_INFORMATION"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().num_arguments(2)
    }

    fn schema(&self) -> Arc<DataSchema> {
        Arc::new(ClusteringInformation::schema().into())
    }
}

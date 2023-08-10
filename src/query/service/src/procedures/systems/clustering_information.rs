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
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_procedures::ProcedureFeatures;
use common_procedures::ProcedureSignature;

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::fuse::table_functions::ClusteringInformation;
use crate::storages::fuse::FuseTable;

pub struct ClusteringInformationProcedure {
    sig: Box<dyn ProcedureSignature>,
}

impl ClusteringInformationProcedure {
    pub fn try_create(sig: Box<dyn ProcedureSignature>) -> Result<Box<dyn Procedure>> {
        Ok(ClusteringInformationProcedure { sig }.into_procedure())
    }
}

impl ProcedureSignature for ClusteringInformationProcedure {
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
impl OneBlockProcedure for ClusteringInformationProcedure {
    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        assert_eq!(args.len(), 2);
        let database_name = args[0].clone();
        let table_name = args[1].clone();
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(&ctx.get_current_catalog())
            .await?
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        Ok(ClusteringInformation::new(ctx, tbl)
            .get_clustering_info()
            .await?)
    }
}

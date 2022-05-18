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

use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::storages::fuse::table_functions::get_cluster_keys;
use crate::storages::fuse::table_functions::ClusteringInformation;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;

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
        // Todo(zhyass): ProcedureFeatures::default().variadic_arguments(2, 3)
        ProcedureFeatures::default().num_arguments(2)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let database_name = args[0].clone();
        let table_name = args[1].clone();
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(ctx.get_current_catalog())?
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        let tbl = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "expecting fuse table, but got table of engine type: {}",
                tbl.get_table_info().meta.engine
            ))
        })?;

        let cluster_keys = if args.len() == 2 {
            tbl.cluster_keys()
        } else {
            get_cluster_keys(ctx.clone(), tbl.schema(), &args[2]).await?
        };

        if cluster_keys.is_empty() {
            return Err(ErrorCode::InvalidClusterKeys(format!(
                "Invalid clustering keys or table {} is not clustered",
                table_name
            )));
        }

        Ok(ClusteringInformation::new(ctx, tbl, cluster_keys)
            .get_clustering_info()
            .await?)
    }

    fn schema(&self) -> Arc<DataSchema> {
        ClusteringInformation::schema()
    }
}

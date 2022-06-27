//  Copyright 2021 Datafuse Labs.
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
use crate::storages::fuse::table_functions::FuseSnapshot;
use crate::storages::fuse::FuseTable;

pub struct FuseSnapshotProcedure {}

impl FuseSnapshotProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(FuseSnapshotProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for FuseSnapshotProcedure {
    fn name(&self) -> &str {
        "FUSE_SNAPSHOT"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().variadic_arguments(2, 3)
    }

    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let catalog_name = ctx.get_current_catalog();
        let tenant_id = ctx.get_tenant();
        let database_name = args[0].clone();
        let table_name = args[1].clone();

        let limit = if args.len() > 2 {
            let size = args[2].parse::<usize>()?;
            Some(size)
        } else {
            None
        };
        let tbl = ctx
            .get_catalog(&catalog_name)?
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        Ok(FuseSnapshot::new(ctx, tbl).get_history(limit).await?)
    }

    fn schema(&self) -> Arc<DataSchema> {
        FuseSnapshot::schema()
    }
}

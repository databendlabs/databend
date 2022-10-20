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

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::fuse::table_functions::FuseBlock;
use crate::storages::fuse::FuseTable;

pub struct FuseBlockProcedure {}

impl FuseBlockProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(FuseBlockProcedure {}.into_procedure())
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for FuseBlockProcedure {
    fn name(&self) -> &str {
        "FUSE_BLOCK"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().variadic_arguments(2, 3)
    }

    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let database_name = args[0].clone();
        let table_name = args[1].clone();
        let snapshot_id = if args.len() > 2 {
            Some(args[2].clone())
        } else {
            None
        };
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(&ctx.get_current_catalog())?
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        Ok(FuseBlock::new(ctx, tbl, snapshot_id).get_blocks().await?)
    }

    fn schema(&self) -> Arc<DataSchema> {
        FuseBlock::schema()
    }
}

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

use common_datavalues::DataSchema;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

use crate::procedures::procedure::ProcedureStream;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::fuse::table_functions::FuseSnapshot;

pub struct FuseSnapshotProcedure {}

impl FuseSnapshotProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        // Ok(Box::new(FuseSnapshotProcedure {}.to_procedure()))
        Ok(FuseSnapshotProcedure {}.into_procedure())
    }
}

#[async_trait::async_trait]
impl ProcedureStream for FuseSnapshotProcedure {
    fn name(&self) -> &str {
        "FUSE_SNAPSHOT"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default().variadic_arguments(2, 3)
    }

    async fn data_stream(
        &self,
        ctx: Arc<QueryContext>,
        args: Vec<String>,
    ) -> Result<SendableDataBlockStream> {
        let catalog_name = ctx.get_current_catalog();
        let tenant_id = ctx.get_tenant();
        let database_name = args[0].clone();
        let table_name = args[1].clone();

        let limit = args.get(2).map(|arg| arg.parse::<usize>()).transpose()?;
        let tbl = ctx
            .get_catalog(&catalog_name)?
            .get_table(
                tenant_id.as_str(),
                database_name.as_str(),
                table_name.as_str(),
            )
            .await?;

        FuseSnapshot::new(ctx, tbl).get_history_stream_as_blocks(limit)
    }

    fn schema(&self) -> Arc<DataSchema> {
        FuseSnapshot::schema()
    }
}

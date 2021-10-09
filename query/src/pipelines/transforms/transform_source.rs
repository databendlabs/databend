// Copyright 2020 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_streams::CorrectWithSchemaStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::DatabendQueryContextRef;

pub struct SourceTransform {
    ctx: DatabendQueryContextRef,
    source_plan: ReadDataSourcePlan,
}

impl SourceTransform {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        source_plan: ReadDataSourcePlan,
    ) -> Result<Self> {
        Ok(SourceTransform { ctx, source_plan })
    }

    async fn read_table(&self, _db: &str) -> Result<SendableDataBlockStream> {
        let table_id = self.source_plan.table_info.table_id;
        let table_ver = self.source_plan.table_info.version;
        let table = if self.source_plan.tbl_args.is_none() {
            self.ctx
                .get_table_by_id(table_id, Some(table_ver))?
                .raw()
                .clone()
        } else {
            let func_meta = self.ctx.get_table_function(
                &self.source_plan.table_info.name,
                self.source_plan.tbl_args.clone(),
            )?;
            func_meta.raw().clone().as_table()
        };
        // TODO(xp): get_single_node_table_io_context() or
        //           get_cluster_table_io_context()?
        let io_ctx = Arc::new(self.ctx.get_cluster_table_io_context()?);
        let table_stream = table.read(io_ctx, &self.source_plan.push_downs);
        Ok(Box::pin(
            self.ctx.try_create_abortable(table_stream.await?)?,
        ))
    }
}

#[async_trait::async_trait]
impl Processor for SourceTransform {
    fn name(&self) -> &str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call SourceTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let db = self.source_plan.table_info.db.clone();
        let table = self.source_plan.table_info.name.clone();

        tracing::debug!("execute, table:{:#}.{:#} ...", db, table);

        // We need to keep the block struct with the schema
        // Because the table may not support require columns
        Ok(Box::pin(CorrectWithSchemaStream::new(
            self.read_table(&db).await?,
            self.source_plan.table_info.schema.clone(),
        )))
    }
}

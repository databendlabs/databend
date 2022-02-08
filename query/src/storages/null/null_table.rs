// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing::info;
use futures::stream::StreamExt;

use crate::sessions::QueryContext;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub struct NullTable {
    table_info: TableInfo,
}

impl NullTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(Self { table_info }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "NULL".to_string(),
            comment: "NULL Storage Engine".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Table for NullTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.table_info.schema());

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }

    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        mut stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        while let Some(block) = stream.next().await {
            let block = block?;
            info!("Ignore one block rows: {}", block.num_rows())
        }
        Ok(Box::pin(DataBlockStream::create(
            std::sync::Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Ok(())
    }
}

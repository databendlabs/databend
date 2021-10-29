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

use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::TableOptions;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;

pub struct ExampleTable {
    table_info: TableInfo,
}

impl ExampleTable {
    #[allow(dead_code)]
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        options: TableOptions,
        table_id: u64,
    ) -> Result<Box<dyn Table>> {
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", db, name),
            name,

            meta: TableMeta {
                schema,
                engine: "ExampleNull".to_string(),
                options,
            },
        };

        Ok(Box::new(Self { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for ExampleTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.schema());

        Ok(Box::pin(DataBlockStream::create(
            self.schema(),
            None,
            vec![block],
        )))
    }

    async fn append_data(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Ok(())
    }

    async fn truncate(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Ok(())
    }
}

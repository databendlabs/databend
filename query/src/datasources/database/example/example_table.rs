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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContextRef;

pub struct ExampleTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    table_id: u64,
}

impl ExampleTable {
    #[allow(dead_code)]
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        _options: TableOptions,
        table_id: u64,
    ) -> Result<Box<dyn Table>> {
        let table = Self {
            db,
            name,
            schema,
            table_id,
        };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl Table for ExampleTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn database(&self) -> &str {
        self.db.as_str()
    }

    fn engine(&self) -> &str {
        "ExampleNull"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: DatabendQueryContextRef,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            table_id: self.table_id,
            table_version: None,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::new_exact(0, 0),
            description: format!(
                "(Read from Example Null Engine table  {}.{})",
                self.db, self.name
            ),
            scan_plan: Default::default(), // scan_plan will be removed form ReadSourcePlan soon
            remote: false,
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        _ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.schema.clone());

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }

    async fn append_data(
        &self,
        _ctx: DatabendQueryContextRef,
        _insert_plan: common_planners::InsertIntoPlan,
    ) -> Result<()> {
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: DatabendQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Ok(())
    }
}

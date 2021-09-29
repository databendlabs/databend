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
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContextRef;

pub struct ContributorsTable {
    table_id: u64,
    db_name: String,
    schema: DataSchemaRef,
}

impl ContributorsTable {
    pub fn create(table_id: u64, db_name: &str) -> Self {
        ContributorsTable {
            table_id,
            db_name: db_name.to_string(),
            schema: DataSchemaRefExt::create(vec![DataField::new("name", DataType::String, false)]),
        }
    }
}

#[async_trait::async_trait]
impl Table for ContributorsTable {
    fn name(&self) -> &str {
        "contributors"
    }

    fn database(&self) -> &str {
        self.db_name.as_str()
    }

    fn engine(&self) -> &str {
        "SystemContributors"
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
            db: "system".to_string(),
            table: self.name().to_string(),
            table_id: self.table_id,
            table_version: None,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.contributors table)".to_string(),
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
        let contributors: Vec<&[u8]> = env!("DATABEND_COMMIT_AUTHORS")
            .split_terminator(',')
            .map(|x| x.trim().as_bytes())
            .collect();
        let block =
            DataBlock::create_by_array(self.schema.clone(), vec![Series::new(contributors)]);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}

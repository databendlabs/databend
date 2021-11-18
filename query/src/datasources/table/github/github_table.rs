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

use common_context::DataContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;

const REPOSITORY: &str = "reposiroty";
const LANGUAGE: &str = "language";
const LICENSE: &str = "license";
const STAR_COUNT: &str = "star_count";
const FORKS_COUNT: &str = "forks_count";
const WATCHERS_COUNT: &str = "watchers_count";
const OPEN_ISSUES_COUNT: &str = "open_issues_count";
const SUBSCRIBERS_COUNT: &str = "subscribers_count";
const CREATED_AT: &str = "created_at";
const UPDATED_AT: &str = "updated_at";

pub struct GithubTable {
    table_info: TableInfo,
}

impl GithubTable {
    pub fn try_create(
        table_info: TableInfo,
        _data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        let schema = GithubTable::init_schema();
        let table_info = table_info.set_schema(schema);
        Ok(Box::new(Self { table_info }))
    }

    fn init_schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(REPOSITORY, DataType::String, false),
            DataField::new(LANGUAGE, DataType::String, true),
            DataField::new(LICENSE, DataType::String, true),
            DataField::new(STAR_COUNT, DataType::UInt32, true),
            DataField::new(FORKS_COUNT, DataType::UInt32, true),
            DataField::new(FORKS_COUNT, DataType::UInt32, true),
            DataField::new(WATCHERS_COUNT, DataType::UInt32, true),
            DataField::new(OPEN_ISSUES_COUNT, DataType::UInt32, true),
            DataField::new(SUBSCRIBERS_COUNT, DataType::UInt32, true),
            DataField::new(CREATED_AT, DataType::String, true),
            DataField::new(UPDATED_AT, DataType::String, true),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl Table for GithubTable {
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
        log::info!("try to read from github table");
        let block = DataBlock::empty_with_schema(self.table_info.schema());

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}

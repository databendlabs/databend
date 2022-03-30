// Copyright 2022 Datafuse Labs.
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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub struct ViewTable {
    table_info: TableInfo,
    pub query: String,
}

pub const VIEW_ENGINE: &str = "VIEW";
pub const QUERY: &str = "query";

impl ViewTable {
    pub fn try_create(ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        let query = table_info.options().get(QUERY).cloned();
        if let Some(query) = query {
            Ok(Box::new(ViewTable {
                query,
                table_info,
            }))
        }else {
            Err(ErrorCode::LogicalError("Need `query` when creating ViewTable"))
        }
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "VIEW".to_string(),
            comment: "VIEW STORAGE (LOGICAL VIEW)".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Table for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

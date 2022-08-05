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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;

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
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        let query = table_info.options().get(QUERY).cloned();
        if let Some(query) = query {
            Ok(Box::new(ViewTable { query, table_info }))
        } else {
            Err(ErrorCode::LogicalError(
                "Need `query` when creating ViewTable",
            ))
        }
    }

    /// When using `create`, must provide query in table_info
    pub fn create(table_info: TableInfo) -> Arc<dyn Table> {
        let query = table_info.options().get(QUERY).cloned();
        if let Some(query) = query {
            Arc::new(ViewTable { query, table_info })
        } else {
            panic!("Need `query` when creating ViewTable")
        }
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "VIEW".to_string(),
            comment: "VIEW STORAGE (LOGICAL VIEW)".to_string(),
            ..Default::default()
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

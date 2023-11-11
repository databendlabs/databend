// Copyright 2021 Datafuse Labs
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

use common_catalog::catalog::StorageDescription;
use common_catalog::table::Table;
use common_exception::Result;
use common_meta_app::schema::TableInfo;

pub struct StreamTable {
    table_info: TableInfo,
    pub query: String,
}

pub const STREAM_ENGINE: &str = "VIEW";
pub const QUERY: &str = "query";
pub const APPEND_ONLY: &str = "append_only";

impl StreamTable {
    pub fn try_create(_table_info: TableInfo) -> Result<Box<dyn Table>> {
        todo!()
    }

    pub fn create(_table_info: TableInfo) -> Arc<dyn Table> {
        todo!()
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "VIEW".to_string(),
            comment: "STREAM STORAGE Engine".to_string(),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Table for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

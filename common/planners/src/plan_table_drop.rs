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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_meta_types::DropTableReq;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropTablePlan {
    pub if_exists: bool,
    pub tenant: String,
    pub db: String,
    /// The table name
    pub table: String,
}

impl DropTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl From<DropTablePlan> for DropTableReq {
    fn from(p: DropTablePlan) -> Self {
        DropTableReq {
            if_exists: p.if_exists,
            tenant: p.tenant,
            db: p.db,
            table: p.table,
        }
    }
}

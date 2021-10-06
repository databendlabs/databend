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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;

use crate::Expression;
use crate::Extras;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ScanPlan {
    // The name of the schema
    pub schema_name: String,
    pub table_id: MetaId,
    pub table_version: Option<MetaVersion>,
    // The schema of the source data
    pub table_schema: DataSchemaRef,
    pub table_args: Option<Expression>,
    pub projected_schema: DataSchemaRef,
    // Extras.
    pub push_downs: Extras,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    pub fn with_table_id(table_id: u64, table_version: Option<u64>) -> ScanPlan {
        ScanPlan {
            schema_name: "".to_string(),
            table_id,
            table_version,
            table_schema: Arc::new(DataSchema::empty()),
            projected_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            push_downs: Extras::default(),
        }
    }

    pub fn empty() -> Self {
        Self {
            schema_name: "".to_string(),
            table_id: 0,
            table_version: None,
            table_schema: Arc::new(DataSchema::empty()),
            projected_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            push_downs: Extras::default(),
        }
    }
}

impl Default for ScanPlan {
    fn default() -> Self {
        Self::empty()
    }
}

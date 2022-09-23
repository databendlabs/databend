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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_meta_types::MetaId;
use common_pipeline_sources::processors::sources::input_formats::InputContext;

use super::Plan;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum InsertInputSource {
    #[serde(skip)]
    SelectPlan(Box<Plan>),
    // From outside streaming source
    #[serde(skip)]
    StreamingWithFormat(String, Arc<InputContext>),
    // From cloned String and format
    StrWithFormat((String, String)),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct InsertValueBlock {
    #[serde(skip)]
    pub block: DataBlock,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Insert {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: MetaId,
    pub schema: DataSchemaRef,
    pub overwrite: bool,
    pub source: InsertInputSource,
}

impl PartialEq for Insert {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.database == other.database
            && self.table == other.table
            && self.schema == other.schema
    }
}

impl Insert {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn has_select_plan(&self) -> bool {
        matches!(&self.source, InsertInputSource::SelectPlan(_))
    }

    pub fn format(&self) -> Option<&str> {
        match &self.source {
            InsertInputSource::SelectPlan(_) => None,
            InsertInputSource::StreamingWithFormat(v, _) => Some(v.as_str()),
            InsertInputSource::StrWithFormat((_, v)) => Some(v.as_str()),
        }
    }
}

impl std::fmt::Debug for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Insert")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("table_id", &self.table_id)
            .field("schema", &self.schema)
            .field("overwrite", &self.overwrite)
            .finish()
    }
}

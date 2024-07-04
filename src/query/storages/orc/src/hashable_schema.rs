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

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;

#[derive(Eq, Debug, Clone)]
pub struct HashableSchema {
    pub arrow_schema: SchemaRef,
    pub table_schema: TableSchemaRef,
    pub data_schema: DataSchemaRef,
    key: String,
    hash: u64,
}

impl HashableSchema {
    pub fn try_create(arrow_schema: SchemaRef) -> Result<Self> {
        let table_schema = Arc::new(
            TableSchema::try_from(arrow_schema.as_ref()).map_err(ErrorCode::from_std_error)?,
        );
        let data_schema = Arc::new(DataSchema::from(table_schema.clone()));

        let key = serde_json::to_string(arrow_schema.fields()).unwrap();
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        Ok(Self {
            arrow_schema,
            table_schema,
            data_schema,
            key,
            hash,
        })
    }
}

impl Hash for HashableSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq<Self> for HashableSchema {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

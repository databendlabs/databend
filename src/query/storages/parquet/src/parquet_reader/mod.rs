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

mod deserialize;
mod meta;
mod read;
use std::sync::Arc;

use common_catalog::plan::Projection;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_storage::ColumnLeaves;
use opendal::Operator;
pub use read::IndexedChunk;

#[derive(Clone)]
pub struct ParquetReader {
    operator: Operator,
    projected_schema: DataSchemaRef,
    projected_column_leaves: ColumnLeaves,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        // Full schema and column leaves.
        let arrow_schema = schema.to_arrow();
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        // Projected schema and column leaves.
        let projected_schema = match projection {
            Projection::Columns(ref indices) => DataSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                DataSchemaRef::new(schema.inner_project(path_indices))
            }
        };
        let projected_column_leaves = ColumnLeaves {
            column_leaves: projection
                .project_column_leaves(&column_leaves)?
                .iter()
                .map(|&leaf| leaf.clone())
                .collect(),
        };

        Ok(Arc::new(ParquetReader {
            operator,
            projected_schema,
            projected_column_leaves,
        }))
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }
}

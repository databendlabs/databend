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

use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_catalog::plan::Projection;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_storage::ColumnLeaves;
use opendal::Operator;

mod deserialize;
mod read;

#[derive(Clone)]
pub struct ParquetReader {
    operator: Operator,
    projection: Projection,
    projected_schema: DataSchemaRef,
    column_leaves: ColumnLeaves,
    parquet_schema_descriptor: SchemaDescriptor,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        let projected_schema = match projection {
            Projection::Columns(ref indices) => DataSchemaRef::new(schema.project(indices)),
            Projection::InnerColumns(ref path_indices) => {
                DataSchemaRef::new(schema.inner_project(path_indices))
            }
        };

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        Ok(Arc::new(ParquetReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            column_leaves,
        }))
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    pub fn support_blocking_api(&self) -> bool {
        self.operator.metadata().can_blocking()
    }
}

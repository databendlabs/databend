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
mod filter;
mod meta;
mod read;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::schema_projection as ap;
use common_catalog::plan::Projection;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_storage::ColumnLeaves;
use opendal::Operator;
pub use read::IndexedChunk;

/// The reader to parquet files with a projected schema.
///
/// **ALERT**: dictionary type is not supported yet.
/// If there are dictionary pages in the parquet file, the reading process may fail.
#[derive(Clone)]
pub struct ParquetReader {
    operator: Operator,
    /// The indices of columns need to read by this reader.
    ///
    /// Use [`HashSet`] to avoid duplicate indices.
    /// Duplicate indices will exist when there are nested types or
    /// select a same field multiple times.
    ///
    /// For example:
    ///
    /// ```sql
    /// select a, a.b, a.c from t;
    /// select a, b, a from t;
    /// ```
    columns_to_read: HashSet<usize>,
    /// The schema of the [`common_datablocks::DataBlock`] this reader produces.
    output_schema: DataSchemaRef,
    /// The actual schema used to read parquet. It will be converted to [`common_datavalues::DataSchema`] when output [`common_datablocks::DataBlock`].
    ///
    /// The reason of using [`ArrowSchema`] to read parquet is that
    /// There are some types that Databend not support such as Timestamp of nanoseconds.
    /// Such types will be convert to supported types after deserialization.
    projected_arrow_schema: ArrowSchema,
    /// [`ColumnLeaves`] corresponding to the `projected_schema`.
    projected_column_leaves: ColumnLeaves,
    /// [`ColumnDescriptor`]s corresponding to the `projected_schema`.
    projected_column_descriptors: HashMap<usize, ColumnDescriptor>,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: ArrowSchema,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        let (
            projected_arrow_schema,
            projected_column_leaves,
            projected_column_descriptors,
            columns_to_read,
        ) = Self::do_projection(&schema, &projection)?;

        Ok(Arc::new(ParquetReader {
            operator,
            columns_to_read,
            output_schema: Arc::new(DataSchema::from(&projected_arrow_schema)),
            projected_arrow_schema,
            projected_column_leaves,
            projected_column_descriptors,
        }))
    }

    pub fn output_schema(&self) -> DataSchemaRef {
        self.output_schema.clone()
    }

    pub fn columns_to_read(&self) -> &HashSet<usize> {
        &self.columns_to_read
    }

    /// Project the schema and get the needed column leaves.
    #[allow(clippy::type_complexity)]
    pub fn do_projection(
        schema: &ArrowSchema,
        projection: &Projection,
    ) -> Result<(
        ArrowSchema,
        ColumnLeaves,
        HashMap<usize, ColumnDescriptor>,
        HashSet<usize>,
    )> {
        // Full schema and column leaves.
        let column_leaves = ColumnLeaves::new_from_schema(schema);
        let schema_descriptors = to_parquet_schema(schema)?;
        // Project schema
        let projected_arrow_schema = match projection {
            Projection::Columns(indices) => ap::project(schema, indices),
            Projection::InnerColumns(path_indices) => ap::inner_project(schema, path_indices),
        };
        // Project column leaves
        let projected_column_leaves = ColumnLeaves {
            column_leaves: projection
                .project_column_leaves(&column_leaves)?
                .iter()
                .map(|&leaf| leaf.clone())
                .collect(),
        };
        let column_leaves = &projected_column_leaves.column_leaves;
        // Project column descriptors and collect columns to read
        let mut projected_column_descriptors = HashMap::with_capacity(column_leaves.len());
        let mut columns_to_read =
            HashSet::with_capacity(column_leaves.iter().map(|leaf| leaf.leaf_ids.len()).sum());
        for column_leaf in column_leaves {
            for index in &column_leaf.leaf_ids {
                columns_to_read.insert(*index);
                projected_column_descriptors
                    .insert(*index, schema_descriptors.columns()[*index].clone());
            }
        }
        Ok((
            projected_arrow_schema,
            projected_column_leaves,
            projected_column_descriptors,
            columns_to_read,
        ))
    }
}

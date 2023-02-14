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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::schema_projection as ap;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_storage::ColumnNodes;
use opendal::Operator;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_table::arrow_to_table_schema;

pub trait SeekRead: std::io::Read + std::io::Seek {}

impl<T> SeekRead for T where T: std::io::Read + std::io::Seek {}

pub struct DataReader {
    bytes: usize,
    inner: Box<dyn SeekRead + Sync + Send>,
}

impl DataReader {
    pub fn new(inner: Box<dyn SeekRead + Sync + Send>, bytes: usize) -> Self {
        Self { inner, bytes }
    }

    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(self.bytes);
        // `DataReader` might be reused if there is nested-type data, example:
        // Table: t Tuple(a int, b int);
        // Query: select t from table where t:a > 1;
        // The query will create two readers: Reader(a), Reader(b).
        // Prewhere phase: Reader(a).read_all();
        // Remain phase: Reader(a).read_all(); Reader(b).read_all();
        // If we don't seek to the start of the reader, the second read_all will read nothing.
        self.inner.rewind()?;
        // TODO(1): don't seek and read, but reuse the data (reduce IO).
        // TODO(2): for nested types, merge sub columns into one column (reduce deserialization).
        self.inner.read_to_end(&mut data)?;
        Ok(data)
    }
}

pub type IndexedChunk = (FieldIndex, Vec<u8>);
pub type IndexedReaders = HashMap<FieldIndex, DataReader>;

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
    columns_to_read: HashSet<FieldIndex>,
    /// The schema of the [`common_expression::DataBlock`] this reader produces.
    ///
    /// ```
    /// output_schema = DataSchema::from(projected_arrow_schema)
    /// ```
    pub(crate) output_schema: DataSchemaRef,
    /// The actual schema used to read parquet.
    ///
    /// The reason of using [`ArrowSchema`] to read parquet is that
    /// There are some types that Databend not support such as Timestamp of nanoseconds.
    /// Such types will be convert to supported types after deserialization.
    pub(crate) projected_arrow_schema: ArrowSchema,
    /// [`ColumnNodes`] corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_nodes: ColumnNodes,
    /// [`ColumnDescriptor`]s corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_descriptors: HashMap<FieldIndex, ColumnDescriptor>,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: ArrowSchema,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        let (
            projected_arrow_schema,
            projected_column_nodes,
            projected_column_descriptors,
            columns_to_read,
        ) = Self::do_projection(&schema, &projection)?;

        let t_schema = arrow_to_table_schema(projected_arrow_schema.clone());
        let output_schema = DataSchema::from(&t_schema);

        Ok(Arc::new(ParquetReader {
            operator,
            columns_to_read,
            output_schema: Arc::new(output_schema),
            projected_arrow_schema,
            projected_column_nodes,
            projected_column_descriptors,
        }))
    }

    /// Project the schema and get the needed column leaves.
    #[allow(clippy::type_complexity)]
    pub fn do_projection(
        schema: &ArrowSchema,
        projection: &Projection,
    ) -> Result<(
        ArrowSchema,
        ColumnNodes,
        HashMap<FieldIndex, ColumnDescriptor>,
        HashSet<FieldIndex>,
    )> {
        // Full schema and column leaves.

        let column_nodes = ColumnNodes::new_from_schema(schema, None);
        let schema_descriptors = to_parquet_schema(schema)?;
        // Project schema
        let projected_arrow_schema = match projection {
            Projection::Columns(indices) => ap::project(schema, indices),
            Projection::InnerColumns(path_indices) => ap::inner_project(schema, path_indices),
        };
        // Project column leaves
        let projected_column_nodes = ColumnNodes {
            column_nodes: projection
                .project_column_nodes(&column_nodes)?
                .iter()
                .map(|&leaf| leaf.clone())
                .collect(),
        };
        let column_nodes = &projected_column_nodes.column_nodes;
        // Project column descriptors and collect columns to read
        let mut projected_column_descriptors = HashMap::with_capacity(column_nodes.len());
        let mut columns_to_read = HashSet::with_capacity(
            column_nodes
                .iter()
                .map(|leaf| leaf.leaf_indices.len())
                .sum(),
        );
        for column_node in column_nodes {
            for index in &column_node.leaf_indices {
                columns_to_read.insert(*index);
                projected_column_descriptors
                    .insert(*index, schema_descriptors.columns()[*index].clone());
            }
        }
        Ok((
            projected_arrow_schema,
            projected_column_nodes,
            projected_column_descriptors,
            columns_to_read,
        ))
    }

    pub fn read_from_readers(&self, readers: &mut IndexedReaders) -> Result<Vec<IndexedChunk>> {
        let mut chunks = Vec::with_capacity(self.columns_to_read.len());

        for index in &self.columns_to_read {
            let reader = readers.get_mut(index).unwrap();
            let data = reader.read_all()?;

            chunks.push((*index, data));
        }

        Ok(chunks)
    }

    pub fn readers_from_blocking_io(&self, part: PartInfoPtr) -> Result<IndexedReaders> {
        let part = ParquetRowGroupPart::from_part(&part)?;

        let mut readers: HashMap<usize, DataReader> =
            HashMap::with_capacity(self.columns_to_read.len());

        for index in &self.columns_to_read {
            let obj = self.operator.object(&part.location);
            let meta = &part.column_metas[index];
            let reader = obj.blocking_range_reader(meta.offset..meta.offset + meta.length)?;
            readers.insert(
                *index,
                DataReader::new(Box::new(reader), meta.length as usize),
            );
        }
        Ok(readers)
    }

    pub async fn readers_from_non_blocking_io(&self, part: PartInfoPtr) -> Result<IndexedReaders> {
        let part = ParquetRowGroupPart::from_part(&part)?;

        let mut join_handlers = Vec::with_capacity(self.columns_to_read.len());
        let obj = self.operator.object(&part.location);

        for index in self.columns_to_read.iter() {
            let meta = &part.column_metas[index];
            let obj = obj.clone();
            let (offset, length) = (meta.offset, meta.length);

            join_handlers.push(async move {
                let data = obj.range_read(offset..offset + length).await?;
                Ok::<_, ErrorCode>((
                    *index,
                    DataReader::new(Box::new(std::io::Cursor::new(data)), length as usize),
                ))
            });
        }

        let res = futures::future::try_join_all(join_handlers).await?;

        Ok(res.into_iter().collect())
    }
}

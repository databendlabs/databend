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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use common_arrow::arrow::bitmap::Bitmap;
use common_catalog::plan::Projection;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use opendal::Operator;
use parquet::file::metadata::RowGroupMetaData;
use parquet::schema::types::ColumnDescPtr;
use parquet::schema::types::SchemaDescPtr;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_rs::column_nodes::ColumnNodesRS;
use crate::parquet_rs::convert::convert_column_meta;
use crate::parquet_rs::parquet_reader::row_group_reader::bitmap_to_selection;
use crate::parquet_rs::parquet_reader::row_group_reader::InMemoryRowGroup;
use crate::parquet_rs::parquet_table::arrow_to_table_schema;
use crate::parquet_rs::projection::project_schema_all;

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
    pub output_schema: DataSchemaRef,
    /// The actual schema used to read parquet.
    ///
    /// The reason of using [`ArrowSchema`] to read parquet is that
    /// There are some types that Databend not support such as Timestamp of nanoseconds.
    /// Such types will be convert to supported types after deserialization.
    #[allow(dead_code)]
    pub(crate) projected_arrow_schema: ArrowSchema,
    /// [`ColumnNodes`] corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_nodes: ColumnNodesRS,
    pub(crate) projected_schema_descriptor: SchemaDescPtr,
    /// [`ColumnDescriptor`]s corresponding to the `projected_arrow_schema`.
    pub(crate) projected_column_descriptors: HashMap<FieldIndex, ColumnDescPtr>,
}

impl ParquetReader {
    pub fn create(
        operator: Operator,
        schema: &ArrowSchema,
        schema_descr: &SchemaDescPtr,
        projection: Projection,
    ) -> Result<Arc<ParquetReader>> {
        let (
            projected_arrow_schema,
            projected_column_nodes,
            projected_schema_descriptor,
            columns_to_read,
            projected_column_descriptors,
        ) = project_schema_all(schema, schema_descr, &projection)?;

        let t_schema = arrow_to_table_schema(projected_arrow_schema.clone())?;
        let output_schema = DataSchema::from(&t_schema);

        Ok(Arc::new(ParquetReader {
            operator,
            columns_to_read,
            output_schema: Arc::new(output_schema),
            projected_arrow_schema,
            projected_column_nodes,
            projected_schema_descriptor,
            projected_column_descriptors,
        }))
    }
}

/// Project the schema and get the needed column leaves.

#[async_trait::async_trait]
impl crate::parquet_reader::ParquetReader for ParquetReader {
    fn output_schema(&self) -> &DataSchema {
        &self.output_schema
    }

    fn columns_to_read(&self) -> &HashSet<FieldIndex> {
        &self.columns_to_read
    }

    fn operator(&self) -> &Operator {
        &self.operator
    }

    fn deserialize(
        &self,
        part: &ParquetRowGroupPart,
        chunks: Vec<(FieldIndex, Vec<u8>)>,
        filter: Option<Bitmap>,
    ) -> Result<DataBlock> {
        if chunks.is_empty() {
            return Ok(DataBlock::new(vec![], part.num_rows));
        }

        let selection = filter.map(bitmap_to_selection);
        let mut column_chunks = vec![];
        let mut metadatas = vec![];

        let chunk_map: HashMap<FieldIndex, Bytes> = chunks
            .into_iter()
            .map(|(i, v)| (i, Bytes::from(v)))
            .collect();

        let column_nodes = &self.projected_column_nodes.column_nodes;

        for column_node in column_nodes.iter() {
            let indices = &column_node.leaf_indices;
            for index in indices {
                // in `read_parquet` function, there is no `TableSchema`, so index treated as column id
                let column_meta = &part.column_metas[index];
                let column_chunk = chunk_map.get(index).unwrap().clone();
                column_chunks.push(Arc::new(column_chunk));
                metadatas.push(convert_column_meta(
                    column_meta,
                    self.projected_column_descriptors[index].clone(),
                ));
            }
        }
        let metadata = RowGroupMetaData::builder(self.projected_schema_descriptor.clone())
            .set_column_metadata(metadatas)
            .build()
            .unwrap();
        let row_group = InMemoryRowGroup {
            metadata,
            column_chunks,
        };
        let mut reader = row_group
            .get_record_batch_reader(part.num_rows, selection)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        DataBlock::from_record_batch(&batch)
            .map_err(|e| {
                ErrorCode::BadBytes(format!(
                    "Cannot convert record batch to data block, error: {:?}",
                    e
                ))
            })
            .map(|v| v.0)
    }
}

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

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageReader;
use common_base::base::tokio::sync::Semaphore;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;
use common_storages_util::file_meta_data_reader::FileMetaDataReader;
use common_storages_util::retry;
use common_storages_util::retry::Retryable;
use futures::AsyncReadExt;
use opendal::Object;
use opendal::Operator;
use tracing::warn;

use crate::hive_partition::HivePartInfo;
use crate::hive_partition_filler::HivePartitionFiller;

#[derive(Clone)]
pub struct HiveParquetBlockReader {
    operator: Operator,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    parquet_schema_descriptor: SchemaDescriptor,
    hive_partition_filler: Option<HivePartitionFiller>,
}

impl HiveParquetBlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Vec<usize>,
        hive_partition_filler: Option<HivePartitionFiller>,
    ) -> Result<Arc<HiveParquetBlockReader>> {
        let projected_schema = DataSchemaRef::new(schema.project(&projection));

        let arrow_schema = schema.to_arrow();
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema)?;

        Ok(Arc::new(HiveParquetBlockReader {
            operator,
            projection,
            projected_schema,
            parquet_schema_descriptor,
            arrow_schema: Arc::new(arrow_schema),
            hive_partition_filler,
        }))
    }

    fn to_deserialize(
        column_meta: &ColumnChunkMetaData,
        chunk: Vec<u8>,
        rows: usize,
        column_descriptor: &ColumnDescriptor,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let pages = PageReader::new(
            std::io::Cursor::new(chunk),
            column_meta,
            Arc::new(|_, _| true),
            vec![],
        );

        let chunk_size = if let Ok(read_buffer_size_str) = std::env::var("CHUNK_SIZE") {
            read_buffer_size_str.parse::<usize>().unwrap_or_else(|_|
                {
                    tracing::warn!(
                    "invalid value of env var READ_BUFFER_SIZE {read_buffer_size_str}, using default value {rows}",
                );
                    rows
                })
        } else {
            rows
        };

        let primitive_type = &column_descriptor.descriptor.primitive_type;
        let decompressor = BasicDecompressor::new(pages, vec![]);
        Ok(column_iter_to_arrays(
            vec![decompressor],
            vec![primitive_type],
            field,
            Some(chunk_size),
        )?)
    }

    fn get_parquet_column_metadata<'a>(
        row_group: &'a RowGroupMetaData,
        field_name: &str,
    ) -> Result<&'a ColumnChunkMetaData> {
        let column_meta: Vec<&ColumnChunkMetaData> = row_group
            .columns()
            .iter()
            .filter(|x| {
                x.descriptor().path_in_schema[0].to_lowercase() == field_name.to_lowercase()
            })
            .collect();
        if column_meta.is_empty() {
            return Err(ErrorCode::ParquetError(format!(
                "couldn't find column:{} in parquet file",
                field_name
            )));
        } else if column_meta.len() > 1 {
            return Err(ErrorCode::ParquetError(format!(
                "find multi column:{} in parquet file",
                field_name
            )));
        }
        Ok(column_meta[0])
    }

    async fn read_column(
        o: Object,
        offset: u64,
        length: u64,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<u8>> {
        let handler = common_base::base::tokio::spawn(async move {
            let op = || async {
                let mut chunk = vec![0; length as usize];
                // Sine error conversion DO matters: retry depends on the conversion
                // to distinguish transient errors from permanent ones.
                // Explict error conversion is used here, to make the code easy to be followed
                let mut r = o
                    .range_reader(offset..offset + length)
                    .await
                    .map_err(retry::from_io_error)?;
                r.read_exact(&mut chunk).await?;
                Ok(chunk)
            };

            let notify = |e: std::io::Error, duration| {
                warn!(
                    "transient error encountered while reading column, at duration {:?} : {}",
                    duration, e,
                )
            };

            let _semaphore_permit = semaphore.acquire().await.unwrap();
            let chunk = op.retry_with_notify(notify).await?;
            Ok(chunk)
        });

        match handler.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(cause)) => Err(cause),
            Err(cause) => Err(ErrorCode::TokioError(format!(
                "Cannot join future {:?}",
                cause
            ))),
        }
    }

    pub async fn read_meta_data(
        &self,
        ctx: Arc<dyn TableContext>,
        filename: &str,
    ) -> Result<Arc<FileMetaData>> {
        let reader = FileMetaDataReader::new_reader(ctx);
        reader.read(filename, None, 0).await
    }

    pub async fn read_columns_data(
        &self,
        row_group: &RowGroupMetaData,
        part: &HivePartInfo,
    ) -> Result<Vec<Vec<u8>>> {
        let mut join_handlers = Vec::with_capacity(self.projection.len());

        let semaphore = Arc::new(Semaphore::new(10));
        for index in &self.projection {
            let field = &self.arrow_schema.fields[*index];
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;
            let (start, len) = column_meta.byte_range();

            join_handlers.push(Self::read_column(
                self.operator.object(&part.filename),
                start,
                len,
                semaphore.clone(),
            ));
        }

        futures::future::try_join_all(join_handlers).await
    }

    pub fn create_rowgroup_deserializer(
        &self,
        chunks: Vec<Vec<u8>>,
        row_group: &RowGroupMetaData,
        _part: HivePartInfo,
    ) -> Result<RowGroupDeserializer> {
        if self.projection.len() != chunks.len() {
            return Err(ErrorCode::LogicalError(
                "Columns chunk len must be equals projections len.",
            ));
        }

        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        for (index, column_chunk) in chunks.into_iter().enumerate() {
            let idx = self.projection[index];
            let field = self.arrow_schema.fields[idx].clone();
            let column_descriptor = &self.parquet_schema_descriptor.columns()[idx];
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;

            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                column_chunk,
                row_group.num_rows(),
                column_descriptor,
                field,
            )?);
        }

        Ok(RowGroupDeserializer::new(
            columns_array_iter,
            row_group.num_rows(),
            None,
        ))
    }

    pub fn create_data_block(
        &self,
        deserializer: &mut RowGroupDeserializer,
        part: HivePartInfo,
        num_rows: usize,
    ) -> Result<Option<DataBlock>> {
        let data_block = self.try_next_block(deserializer).map_err(|err| {
            ErrorCode::ParquetError(format!(
                "deseriallize parquet failed, {}, {:?}",
                part.filename, err
            ))
        })?;
        if let Some(data_block) = data_block {
            match &self.hive_partition_filler {
                Some(hive_partition_filler) => Ok(Some(
                    hive_partition_filler.fill_data(data_block, part, num_rows)?,
                )),
                None => Ok(Some(data_block)),
            }
        } else {
            Ok(data_block)
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self, _part: PartInfoPtr) -> Result<DataBlock> {
        Err(ErrorCode::UnImplement("depracated"))
    }

    fn try_next_block(&self, deserializer: &mut RowGroupDeserializer) -> Result<Option<DataBlock>> {
        match deserializer.next() {
            None => Ok(None),
            Some(Err(cause)) => Err(ErrorCode::from(cause)),
            Some(Ok(chunk)) => Ok(Some(DataBlock::from_chunk(&self.projected_schema, &chunk)?)),
        }
    }
}

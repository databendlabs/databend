//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
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
    hive_partition_filler: Option<HivePartitionFiller>,
}

pub struct DataBlockDeserializer {
    deserializer: RowGroupDeserializer,
    drained: bool,
}

impl DataBlockDeserializer {
    fn new(deserializer: RowGroupDeserializer) -> Self {
        let num_rows = deserializer.num_rows();
        Self {
            deserializer,
            drained: num_rows == 0,
        }
    }

    fn next_block(
        &mut self,
        schema: &DataSchemaRef,
        filler: &Option<HivePartitionFiller>,
        part_info: &HivePartInfo,
    ) -> Result<Option<DataBlock>> {
        if self.drained {
            return Ok(None);
        };

        let opt = self.deserializer.next().transpose()?;
        if let Some(chunk) = opt {
            // If the `Vec<ArrayIter<'static>>` we have passed into the `RowGroupDeserializer`
            // is empty, the deserializer will returns an empty chunk as well(since now rows are consumed).
            // In this case, mark self as drained.
            if chunk.is_empty() {
                self.drained = true;
            }

            let block: DataBlock = DataBlock::from_chunk(schema, &chunk)?;
            return if let Some(filler) = &filler {
                let num_rows = self.deserializer.num_rows();
                let filled = filler.fill_data(block, part_info, num_rows)?;
                Ok(Some(filled))
            } else {
                Ok(Some(block))
            };
        }

        self.drained = true;
        Ok(None)
    }
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
        Ok(Arc::new(HiveParquetBlockReader {
            operator,
            projection,
            projected_schema,
            arrow_schema: Arc::new(arrow_schema),
            hive_partition_filler,
        }))
    }

    fn to_deserialize(
        column_meta: &ColumnChunkMetaData,
        chunk: Vec<u8>,
        rows: usize,
        field: Field,
    ) -> Result<ArrayIter<'static>> {
        let primitive_type = column_meta.descriptor().descriptor.primitive_type.clone();
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

        let decompressor = BasicDecompressor::new(pages, vec![]);
        Ok(column_iter_to_arrays(
            vec![decompressor],
            vec![&primitive_type],
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
    ) -> Result<DataBlockDeserializer> {
        if self.projection.len() != chunks.len() {
            return Err(ErrorCode::LogicalError(
                "Columns chunk len must be equals projections len.",
            ));
        }

        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        for (index, column_chunk) in chunks.into_iter().enumerate() {
            let idx = self.projection[index];
            let field = self.arrow_schema.fields[idx].clone();
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;

            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                column_chunk,
                row_group.num_rows(),
                field,
            )?);
        }

        let num_row = row_group.num_rows();
        let deserializer = RowGroupDeserializer::new(columns_array_iter, num_row, None);
        Ok(DataBlockDeserializer::new(deserializer))
    }

    pub fn create_data_block(
        &self,
        row_group_iterator: &mut DataBlockDeserializer,
        part: HivePartInfo,
    ) -> Result<Option<DataBlock>> {
        row_group_iterator
            .next_block(&self.projected_schema, &self.hive_partition_filler, &part)
            .map_err(|e| e.add_message(format!(" filename of hive part {}", part.filename)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self, _part: PartInfoPtr) -> Result<DataBlock> {
        Err(ErrorCode::UnImplement("deprecated"))
    }
}

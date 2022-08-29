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

use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::read_columns_many_async;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;
use futures::future::BoxFuture;
use futures::io::BufReader;
use opendal::Operator;

use crate::hive_partition::HivePartInfo;
use crate::hive_partition_filler::HivePartitionFiller;

/// default buffer size of BufReader (in bytes)
const DEFAULT_READ_BUFFER_SIZE: usize = 18 * 1024 * 1024;

/// default buffer size of env var name, tmp workaround, for easy of tuning
const READ_BUFFER_SIZE_VAR: &str = "READ_BUFFER_SIZE";

#[derive(Clone)]
pub struct HiveParquetBlockReader {
    operator: Operator,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    hive_partition_filler: Option<HivePartitionFiller>,
    read_buffer_size: usize,
}

impl HiveParquetBlockReader {
    pub fn create(
        operator: Operator,
        schema: DataSchemaRef,
        projection: Vec<usize>,
        hive_partition_filler: Option<HivePartitionFiller>,
    ) -> Result<Arc<HiveParquetBlockReader>> {
        let projected_schema = DataSchemaRef::new(schema.project(&projection));

        let read_buffer_size = if let Ok(read_buffer_size_str) = std::env::var(READ_BUFFER_SIZE_VAR)
        {
            read_buffer_size_str.parse::<usize>().unwrap_or_else(|_|
                {
                    tracing::warn!(
                    "invalid value of env var READ_BUFFER_SIZE {read_buffer_size_str}, using default value {DEFAULT_READ_BUFFER_SIZE}",
                );
                    DEFAULT_READ_BUFFER_SIZE
                })
        } else {
            DEFAULT_READ_BUFFER_SIZE
        };

        let arrow_schema = schema.to_arrow();

        Ok(Arc::new(HiveParquetBlockReader {
            operator,
            projection,
            projected_schema,
            arrow_schema: Arc::new(arrow_schema),
            hive_partition_filler,
            read_buffer_size,
        }))
    }

    pub async fn read_meta_data(&self, filename: &str) -> Result<FileMetaData> {
        let object = self.operator.object(filename);
        let mut reader = object.seekable_reader(0..);
        let meta = read_metadata_async(&mut reader).await;
        if meta.is_err() {
            tracing::warn!("parquet failed,read_meta,{}", filename);
        }
        meta.map_err(|err| {
            ErrorCode::ParquetError(format!("read meta failed, {}, {:?}", filename, err))
        })
    }

    pub async fn read_columns_data(
        &self,
        row_group: &RowGroupMetaData,
        part: &HivePartInfo,
    ) -> Result<Vec<ArrayIter<'static>>> {
        let factory = || {
            Box::pin(async {
                let reader = self.operator.object(&part.filename).seekable_reader(..);
                let buffer_reader = BufReader::with_capacity(self.read_buffer_size, reader);
                Ok(buffer_reader)
            }) as BoxFuture<_>
        };
        let fields = self
            .projection
            .iter()
            .map(|idx| self.arrow_schema.fields[*idx].clone())
            .collect::<Vec<_>>();

        // TODO constraint the degree of concurrency (of reading cols)
        let column_chunks = read_columns_many_async(factory, row_group, fields, None).await?;
        Ok(column_chunks)
    }

    pub fn deserialize_next_block(
        &self,
        row_group_deserializer: &mut RowGroupDeserializer,
        row_group: &RowGroupMetaData,
        part: HivePartInfo,
    ) -> Result<Option<DataBlock>> {
        let maybe_chunk = row_group_deserializer.next().transpose().map_err(|e| {
            ErrorCode::ParquetError(format!(
                "failed to read next chunk of row group, file name {}, err {}",
                part.filename, e
            ))
        })?;

        let maybe_block = maybe_chunk
            .map(|chunk| DataBlock::from_chunk(&self.projected_schema, &chunk))
            .transpose()?;

        // fill the partition column if necessary
        if let Some(filler) = &self.hive_partition_filler {
            if let Some(block) = maybe_block {
                return Ok(Some(filler.fill_data(block, part, row_group.num_rows())?));
            }
        }

        Ok(maybe_block)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self, _part: PartInfoPtr) -> Result<DataBlock> {
        Err(ErrorCode::UnImplement("depracated"))
    }
}

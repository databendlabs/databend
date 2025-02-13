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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;
use databend_storages_common_io::ReadSettings;

use super::native_rows_fetcher::NativeRowsFetcher;
use super::parquet_rows_fetcher::ParquetRowsFetcher;
use crate::FuseStorageFormat;
use crate::FuseTable;

type RowFetcher = Box<dyn Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>>;

pub fn row_fetch_processor(
    ctx: Arc<dyn TableContext>,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
    need_wrap_nullable: bool,
) -> Result<RowFetcher> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection.clone(), false, false, true)?;
    let max_threads = ctx.get_settings().get_max_threads()? as usize;

    match &fuse_table.storage_format {
        FuseStorageFormat::Native => Ok(Box::new(move |input, output| {
            Ok(if block_reader.support_blocking_api() {
                TransformRowsFetcher::create(
                    input,
                    output,
                    row_id_col_offset,
                    max_threads,
                    NativeRowsFetcher::<true>::create(
                        fuse_table.clone(),
                        projection.clone(),
                        block_reader.clone(),
                        max_threads,
                    ),
                    need_wrap_nullable,
                )
            } else {
                TransformRowsFetcher::create(
                    input,
                    output,
                    row_id_col_offset,
                    max_threads,
                    NativeRowsFetcher::<false>::create(
                        fuse_table.clone(),
                        projection.clone(),
                        block_reader.clone(),
                        max_threads,
                    ),
                    need_wrap_nullable,
                )
            })
        })),
        FuseStorageFormat::Parquet => {
            let read_settings = ReadSettings::from_ctx(&ctx)?;
            Ok(Box::new(move |input, output| {
                Ok(if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        max_threads,
                        ParquetRowsFetcher::<true>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                        need_wrap_nullable,
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        max_threads,
                        ParquetRowsFetcher::<false>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                        need_wrap_nullable,
                    )
                })
            }))
        }
    }
}

#[async_trait::async_trait]
pub trait RowsFetcher {
    async fn on_start(&mut self) -> Result<()>;
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock>;
    fn clear_cache(&mut self);
}

pub struct TransformRowsFetcher<F: RowsFetcher> {
    row_id_col_offset: usize,
    max_threads: usize,
    fetcher: F,
    need_wrap_nullable: bool,
    blocks: Vec<DataBlock>,
    row_ids: Vec<u64>,
    distinct_block_ids: HashSet<u64>,
}

#[async_trait::async_trait]
impl<F> AsyncAccumulatingTransform for TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    const NAME: &'static str = "TransformRowsFetcher";

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.fetcher.on_start().await
    }

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let num_rows = data.num_rows();
        let entry = &data.columns()[self.row_id_col_offset];
        let value = entry
            .value
            .convert_to_full_column(&entry.data_type, num_rows);
        let row_id_column = if matches!(entry.data_type, DataType::Number(NumberDataType::UInt64)) {
            value.into_number().unwrap().into_u_int64().unwrap()
        } else {
            // From merge into matched data, the row id column is nullable but has no null value.
            let value = *value.into_nullable().unwrap();
            debug_assert!(value.validity.null_count() == 0);
            value.column.into_number().unwrap().into_u_int64().unwrap()
        };

        // Process the row id column in block batch
        // Ensure that the same block would be processed in the same batch and threads
        let mut consumed_len = num_rows;
        for (idx, row_id) in row_id_column.iter().enumerate() {
            let (prefix, _) = split_row_id(*row_id);

            // Which means we are full now, new prefix will be processed in next batch
            if self.distinct_block_ids.len() >= self.max_threads * 2
                && !self.distinct_block_ids.contains(&prefix)
            {
                consumed_len = idx;
                break;
            }
            self.distinct_block_ids.insert(prefix);
        }

        self.row_ids
            .extend_from_slice(&row_id_column.as_slice()[0..consumed_len]);
        self.blocks.push(data.slice(0..consumed_len));

        if consumed_len < num_rows {
            let block = self.flush().await;
            for row_id in row_id_column.as_slice()[consumed_len..num_rows].iter() {
                let (prefix, _) = split_row_id(*row_id);
                self.distinct_block_ids.insert(prefix);
                self.row_ids.push(*row_id);
            }
            self.blocks.push(data.slice(consumed_len..num_rows));
            block
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        self.flush().await
    }
}

impl<F> TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        max_threads: usize,
        fetcher: F,
        need_wrap_nullable: bool,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncAccumulatingTransformer::create(input, output, Self {
            row_id_col_offset,
            max_threads,
            fetcher,
            need_wrap_nullable,
            blocks: vec![],
            row_ids: vec![],
            distinct_block_ids: HashSet::new(),
        }))
    }

    async fn flush(&mut self) -> Result<Option<DataBlock>> {
        let blocks = std::mem::take(&mut self.blocks);
        if blocks.is_empty() {
            return Ok(None);
        }

        let start_time = std::time::Instant::now();
        let num_blocks = blocks.len();
        let mut data = DataBlock::concat(&blocks)?;
        let num_rows = data.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }

        let row_ids = std::mem::take(&mut self.row_ids);
        self.distinct_block_ids.clear();
        let fetched_block = self.fetcher.fetch(&row_ids).await?;
        // Clear cache after fetch, the block will never be fetched in following batches
        // We ensure it in transform method
        self.fetcher.clear_cache();

        for col in fetched_block.columns().iter() {
            if self.need_wrap_nullable {
                data.add_column(wrap_true_validity(col, num_rows));
            } else {
                data.add_column(col.clone());
            }
        }

        log::info!(
            "TransformRowsFetcher flush: num_rows: {}, input blocks: {} in {} milliseconds",
            num_rows,
            num_blocks,
            start_time.elapsed().as_millis()
        );

        Ok(Some(data))
    }
}

fn wrap_true_validity(column: &BlockEntry, num_rows: usize) -> BlockEntry {
    let (value, data_type) = (&column.value, &column.data_type);
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        let col = NullableColumn::new_column(col, Bitmap::new_trued(num_rows));
        BlockEntry::new(data_type.wrap_nullable(), Value::Column(col))
    }
}

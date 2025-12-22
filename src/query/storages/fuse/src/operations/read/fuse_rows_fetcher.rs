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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_io::ReadSettings;

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

    match &fuse_table.storage_format {
        FuseStorageFormat::Native => unreachable!(),
        FuseStorageFormat::Parquet => {
            let read_settings = ReadSettings::from_ctx(&ctx)?;
            let block_threshold = BlockThreshold {
                max_rows: ctx.get_settings().get_max_block_size()? as usize,
                max_bytes: ctx.get_settings().get_max_block_bytes()? as usize,
                cur_rows: 0,
                cur_bytes: 0,
            };

            Ok(Box::new(move |input, output| {
                Ok(TransformRowsFetcher::create(
                    input,
                    output,
                    row_id_col_offset,
                    ParquetRowsFetcher::create(
                        fuse_table.clone(),
                        projection.clone(),
                        block_reader.clone(),
                        read_settings,
                    ),
                    need_wrap_nullable,
                    block_threshold,
                ))
            }))
        }
    }
}

pub trait RowsFetchMetadata: Send + Sync + 'static {
    fn row_bytes(&self) -> usize;
}

impl<T: RowsFetchMetadata> RowsFetchMetadata for Arc<T> {
    fn row_bytes(&self) -> usize {
        self.as_ref().row_bytes()
    }
}

#[async_trait::async_trait]
pub trait RowsFetcher {
    type Metadata: RowsFetchMetadata;

    async fn initialize(&mut self) -> Result<()>;
    async fn fetch_metadata(&mut self, _block_id: u64) -> Result<Self::Metadata>;

    async fn fetch(
        &mut self,
        row_ids: &[u64],
        metadata: HashMap<u64, Self::Metadata>,
    ) -> Result<DataBlock>;
}

#[derive(Clone, Copy)]
pub struct BlockThreshold {
    max_rows: usize,
    max_bytes: usize,

    cur_rows: usize,
    cur_bytes: usize,
}

impl BlockThreshold {
    pub fn acc(&mut self, rows: usize, bytes: usize) -> bool {
        self.cur_bytes += bytes;
        self.cur_rows += rows;
        self.check_threshold()
    }

    fn check_threshold(&self) -> bool {
        self.cur_rows >= self.max_rows || self.cur_bytes >= self.max_bytes
    }

    fn reset(&mut self) {
        self.cur_rows = 0;
        self.cur_bytes = 0;
    }
}

pub struct TransformRowsFetcher<F: RowsFetcher> {
    fetcher: F,

    initialize: bool,
    finished: bool,
    row_id_col_offset: usize,
    need_wrap_nullable: bool,
    fetch_row_ids: Vec<u64>,
    blocks: Vec<DataBlock>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    block_threshold: BlockThreshold,
    metadata: HashMap<u64, F::Metadata>,
}

#[async_trait::async_trait]
impl<F: RowsFetcher + Send + Sync + 'static> Processor for TransformRowsFetcher<F> {
    fn name(&self) -> String {
        String::from("TransformRowsFetcher")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            return Ok(Event::Finished);
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output_port.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            if !self.finished {
                self.finished = true;
                return Ok(Event::Async);
            }

            self.output_port.finish();
            return Ok(Event::Finished);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.initialize {
            self.initialize = true;
            self.fetcher.initialize().await?;
        }

        if let Some(data) = self.input_data.take() {
            let num_rows = data.num_rows();
            let fetched_columns_bytes = data.memory_size() / data.num_rows();
            let row_id_column = self.get_row_id_column(&data);

            // Process the row id column in block batch
            // Ensure that the same block would be processed in the same batch and threads
            let mut consumed_len = num_rows;
            for (idx, row_id) in row_id_column.iter().enumerate() {
                let (prefix, _) = split_row_id(*row_id);

                if !self.metadata.contains_key(&prefix) {
                    let metadata = self.fetcher.fetch_metadata(prefix).await?;
                    self.metadata.insert(prefix, metadata);
                }

                let fetch_columns_bytes = self.metadata[&prefix].row_bytes();
                let bytes = fetched_columns_bytes + fetch_columns_bytes;
                if self.block_threshold.acc(1, bytes) {
                    consumed_len = idx + 1;
                    break;
                }
            }

            self.blocks.push(data.slice(0..consumed_len));
            self.fetch_row_ids
                .extend_from_slice(&row_id_column.as_slice()[0..consumed_len]);

            let remain_rows = data.slice(consumed_len..num_rows);

            if !remain_rows.is_empty() {
                self.input_data = Some(remain_rows);
            }
        }

        if !self.finished && !self.block_threshold.check_threshold() {
            return Ok(());
        }

        self.block_threshold.reset();
        self.output_data = self.fetch_columns().await?;

        assert!(self.blocks.is_empty());
        assert!(self.metadata.is_empty());

        Ok(())
    }
}

impl<F: RowsFetcher + Send + Sync + 'static> TransformRowsFetcher<F> {
    fn get_row_id_column(&self, data: &DataBlock) -> Buffer<u64> {
        let entry = &data.columns()[self.row_id_col_offset];
        let column = entry.to_column();
        if matches!(entry.data_type(), DataType::Number(NumberDataType::UInt64)) {
            column.into_number().unwrap().into_u_int64().unwrap()
        } else {
            // From merge into matched data, the row id column is nullable but has no null value.
            let value = *column.into_nullable().unwrap();
            debug_assert!(value.validity.null_count() == 0);
            value.column.into_number().unwrap().into_u_int64().unwrap()
        }
    }

    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        fetcher: F,
        need_wrap_nullable: bool,
        block_threshold: BlockThreshold,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(TransformRowsFetcher {
            fetcher,
            block_threshold,
            row_id_col_offset,
            need_wrap_nullable,

            input_port: input,
            output_port: output,
            blocks: vec![],
            input_data: None,
            output_data: None,
            fetch_row_ids: vec![],
            metadata: HashMap::new(),
            finished: false,
            initialize: false,
        }))
    }
}

impl<F: RowsFetcher + Sync + Send + 'static> TransformRowsFetcher<F> {
    async fn fetch_columns(&mut self) -> Result<Option<DataBlock>> {
        let blocks = std::mem::take(&mut self.blocks);
        let row_ids = std::mem::take(&mut self.fetch_row_ids);
        let metadata = std::mem::take(&mut self.metadata);

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

        let fetched_block = self.fetcher.fetch(&row_ids, metadata).await?;

        for mut entry in fetched_block.take_columns() {
            if self.need_wrap_nullable {
                entry = wrap_true_validity(&entry, num_rows);
            }

            data.add_entry(entry);
        }

        log::info!(
            "TransformRowsFetcher flush: num_rows: {}, num_bytes: {}, input blocks: {} in {} milliseconds.",
            num_rows,
            data.memory_size(),
            num_blocks,
            start_time.elapsed().as_millis()
        );

        Ok(Some(data))
    }
}

fn wrap_true_validity(column: &BlockEntry, num_rows: usize) -> BlockEntry {
    let (value, data_type) = (&column.value(), &column.data_type());
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        NullableColumn::new_column(col, Bitmap::new_trued(num_rows)).into()
    }
}

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
use std::collections::VecDeque;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use parquet::file::metadata::RowGroupMetaData;

use crate::sessions::QueryContext;
use crate::spillers::Layout;
use crate::spillers::Location;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;

const MATERIALIZED_CTE_SPILL_UNIT_SIZE: usize = 8 * 1024 * 1024;
/// Maximum number of row groups per file.
const MAX_ROW_GROUPS_PER_FILE: usize = 2 << 15;

#[derive(Clone)]
pub enum MaterializedCtePayload {
    InMemory(DataBlock),
    Spilled(MaterializedCteSpilledPayload),
}

#[derive(Clone)]
pub struct MaterializedCteSpilledPayload {
    path: String,
    row_groups: Arc<Vec<RowGroupMetaData>>,
    schema: DataSchemaRef,
}

impl MaterializedCteSpilledPayload {
    fn restore(self) -> Result<DataBlock> {
        if self.row_groups.is_empty() {
            return Err(ErrorCode::Internal(
                "Failed to restore materialized cte spilled block: empty row groups",
            ));
        }

        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let settings = ReadSettings::default();
        let mut reader = buffer_pool.reader(
            operator,
            self.path,
            self.schema,
            (*self.row_groups).clone(),
            target,
            settings,
        )?;

        let mut blocks = Vec::new();
        while let Some(block) = reader.read()? {
            blocks.push(block);
        }

        match blocks.len() {
            0 => Err(ErrorCode::Internal(
                "Failed to restore materialized cte spilled block",
            )),
            1 => Ok(blocks.pop().unwrap()),
            _ => DataBlock::concat(&blocks),
        }
    }
}

pub struct MaterializedCteSink {
    ctx: Arc<QueryContext>,
    input: Arc<InputPort>,
    senders: Vec<Sender<MaterializedCtePayload>>,
    prefix: String,
    writer_pool_bytes: usize,
    memory_settings: MemorySettings,
    input_data: Option<DataBlock>,
    pending_payloads: VecDeque<MaterializedCtePayload>,
    spilling_blocks: Vec<DataBlock>,
    spilling_bytes: usize,
    closed: bool,
}

impl MaterializedCteSink {
    pub fn create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        senders: Vec<Sender<MaterializedCtePayload>>,
        settings: &Settings,
        memory_settings: MemorySettings,
    ) -> Result<ProcessorPtr> {
        let prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024);
        Ok(ProcessorPtr::create(Box::new(Self {
            ctx,
            input,
            senders,
            prefix,
            writer_pool_bytes,
            memory_settings,
            input_data: None,
            pending_payloads: VecDeque::new(),
            spilling_blocks: vec![],
            spilling_bytes: 0,
            closed: false,
        })))
    }

    fn spill_unit_size(&self) -> usize {
        self.memory_settings
            .spill_unit_size
            .max(MATERIALIZED_CTE_SPILL_UNIT_SIZE)
    }

    fn spill_data_blocks(
        &self,
        data_blocks: Vec<DataBlock>,
    ) -> Result<Vec<MaterializedCtePayload>> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let path = format!("{}/{}", self.prefix, GlobalUniq::unique());
        let mut writer =
            buffer_pool.writer(operator, path.clone(), self.writer_pool_bytes, target)?;

        let schema = Arc::new(data_blocks[0].infer_schema());
        let mut row_group_ranges = Vec::with_capacity(data_blocks.len());
        let mut next_row_group = 0;
        for block in data_blocks {
            writer.write(block)?;
            let row_group_count = writer.flush_row_groups()?;
            row_group_ranges.push(next_row_group..row_group_count);
            next_row_group = row_group_count;
        }
        let (bytes_written, row_groups) = writer.close()?;
        if bytes_written > 0 {
            self.ctx.add_spill_file(
                Location::Remote(path.clone()),
                Layout::Parquet,
                bytes_written,
            );
        }
        Ok(row_group_ranges
            .into_iter()
            .map(|range| {
                MaterializedCtePayload::Spilled(MaterializedCteSpilledPayload {
                    path: path.clone(),
                    row_groups: Arc::new(row_groups[range].to_vec()),
                    schema: schema.clone(),
                })
            })
            .collect())
    }

    fn consume_block(&mut self, data_block: DataBlock) -> Result<()> {
        let can_spill_block = data_block.num_columns() > 0 && !data_block.is_empty();
        if can_spill_block && self.memory_settings.check_spill() {
            self.spilling_bytes += data_block.memory_size();
            self.spilling_blocks.push(data_block);

            if self.spilling_bytes >= self.spill_unit_size()
                || self.spilling_blocks.len() >= MAX_ROW_GROUPS_PER_FILE
            {
                self.flush_spilling_blocks()?;
            }
        } else {
            self.flush_spilling_blocks()?;
            self.pending_payloads
                .push_back(MaterializedCtePayload::InMemory(data_block));
        }
        Ok(())
    }

    async fn send_payload(&self, payload: MaterializedCtePayload) -> Result<()> {
        for sender in self.senders.iter() {
            sender.send(payload.clone()).await.map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        }
        Ok(())
    }

    fn flush_spilling_blocks(&mut self) -> Result<()> {
        if self.spilling_blocks.is_empty() {
            return Ok(());
        }

        let spilling_blocks = std::mem::take(&mut self.spilling_blocks);
        self.spilling_bytes = 0;
        let payloads = self.spill_data_blocks(spilling_blocks)?;
        for payload in payloads {
            self.pending_payloads.push_back(payload);
        }
        Ok(())
    }

    fn close_senders(&mut self) {
        if !self.closed {
            for sender in self.senders.iter() {
                sender.close();
            }
            self.closed = true;
        }
    }
}

impl Drop for MaterializedCteSink {
    fn drop(&mut self) {
        self.close_senders();
    }
}

#[async_trait::async_trait]
impl Processor for MaterializedCteSink {
    fn name(&self) -> String {
        "MaterializedCteSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.pending_payloads.is_empty() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if self.input_data.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.input.finish();
            if !self.spilling_blocks.is_empty() {
                return Ok(Event::Sync);
            }

            self.close_senders();
            return Ok(Event::Finished);
        }

        match self.input.has_data() {
            true => {
                let data = self.input.pull_data().ok_or_else(|| {
                    ErrorCode::Internal("Failed to pull data from input port in materialized cte")
                })??;
                self.input_data = Some(data);
                self.input.set_not_need_data();
                Ok(Event::Sync)
            }
            false => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            self.consume_block(data_block)?;
        } else {
            self.flush_spilling_blocks()?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        while let Some(payload) = self.pending_payloads.pop_front() {
            self.send_payload(payload).await?;
        }
        Ok(())
    }
}

pub struct CTESource {
    receiver: Receiver<MaterializedCtePayload>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    received_payload: Option<MaterializedCtePayload>,
    generated_data: Option<DataBlock>,
    is_finished: bool,
}

impl CTESource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        receiver: Receiver<MaterializedCtePayload>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            receiver,
            output: output_port,
            scan_progress: ctx.get_scan_progress(),
            received_payload: None,
            generated_data: None,
            is_finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CTESource {
    fn name(&self) -> String {
        "MaterializeCTESource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            self.is_finished = true;
            self.receiver.close();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.generated_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.received_payload.is_some() {
            return Ok(Event::Sync);
        }

        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(payload) = self.received_payload.take() {
            let data_block = match payload {
                MaterializedCtePayload::InMemory(data) => data,
                MaterializedCtePayload::Spilled(spilled) => spilled.restore()?,
            };

            if !data_block.is_empty() {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytes,
                    data_block.memory_size(),
                );
                self.generated_data = Some(data_block);
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.receiver.recv().await {
            Ok(payload) => self.received_payload = Some(payload),
            Err(_) => self.is_finished = true,
        }
        Ok(())
    }
}

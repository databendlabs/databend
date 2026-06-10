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
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;
use databend_storages_common_cache::TempDirManager;

use crate::sessions::QueryContext;
use crate::sessions::TableContextQueryIdentity;
use crate::spillers::BackpressureSpiller;
use crate::spillers::SpillReader;
use crate::spillers::SpillWriter;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;
use crate::spillers::SpillsBufferPool;

pub type MaterializedCteSpiller = BackpressureSpiller;

const MATERIALIZED_CTE_SPILL_UNIT_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub enum MaterializedCtePayload {
    InMemory(DataBlock),
    Spilled(MaterializedCteSpilledPayload),
}

#[derive(Clone)]
pub struct MaterializedCteSpilledPayload {
    reader: SpillReader,
    row_group: usize,
}

impl MaterializedCteSpilledPayload {
    fn restore(mut self) -> Result<DataBlock> {
        let blocks = self.reader.restore(vec![self.row_group], usize::MAX)?;
        match blocks.len() {
            0 => Err(ErrorCode::Internal(
                "Failed to restore materialized cte spilled block",
            )),
            1 => Ok(blocks.into_iter().next().unwrap()),
            _ => DataBlock::concat(&blocks),
        }
    }
}

pub fn create_materialized_cte_spiller(
    ctx: Arc<QueryContext>,
    settings: Arc<Settings>,
) -> Result<MaterializedCteSpiller> {
    let temp_dir_manager = TempDirManager::instance();
    let disk_bytes_limit = GlobalConfig::instance()
        .spill
        .materialized_cte_spill_bytes_limit();
    let enable_dio = settings.get_enable_dio()?;
    let disk_spill = temp_dir_manager
        .get_disk_spill_dir(disk_bytes_limit, &ctx.get_id())
        .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
        .transpose()?;

    let config = SpillerConfig {
        spiller_type: SpillerType::MaterializedCTE,
        location_prefix: ctx.query_id_spill_prefix(),
        disk_spill,
        use_parquet: settings.get_spilling_file_format()?.is_parquet(),
        writer_pool_bytes: settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024),
    };
    let operator = DataOperator::instance().spill_operator();
    BackpressureSpiller::create(
        ctx,
        operator,
        config,
        SpillsBufferPool::instance(),
        MATERIALIZED_CTE_SPILL_UNIT_SIZE,
    )
}

pub struct MaterializedCteSink {
    input: Arc<InputPort>,
    senders: Vec<Sender<MaterializedCtePayload>>,
    spiller: MaterializedCteSpiller,
    memory_settings: MemorySettings,
    input_data: Option<DataBlock>,
    pending_payloads: VecDeque<MaterializedCtePayload>,
    spilling_blocks: Vec<DataBlock>,
    spilling_bytes: usize,
    closed: bool,
}

impl MaterializedCteSink {
    pub fn create(
        input: Arc<InputPort>,
        senders: Vec<Sender<MaterializedCtePayload>>,
        spiller: MaterializedCteSpiller,
        memory_settings: MemorySettings,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            senders,
            spiller,
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
        let schema = Arc::new(data_blocks[0].infer_schema());
        let mut writer_creator = self.spiller.new_writer_creator(schema)?;
        let mut pending_blocks = Vec::new();
        let mut pending_size = 0;
        let mut payloads = Vec::with_capacity(data_blocks.len());

        for data_block in data_blocks {
            pending_size += data_block.memory_size();
            pending_blocks.push(data_block);

            if pending_blocks.len() >= SpillWriter::MAX_ROW_GROUPS_PER_FILE {
                self.spill_data_block_chunk(
                    &mut writer_creator,
                    std::mem::take(&mut pending_blocks),
                    pending_size,
                    &mut payloads,
                )?;
                pending_size = 0;
            }
        }

        if !pending_blocks.is_empty() {
            self.spill_data_block_chunk(
                &mut writer_creator,
                pending_blocks,
                pending_size,
                &mut payloads,
            )?;
        }

        Ok(payloads)
    }

    fn spill_data_block_chunk(
        &self,
        writer_creator: &mut crate::spillers::WriterCreator,
        data_blocks: Vec<DataBlock>,
        data_size: usize,
        payloads: &mut Vec<MaterializedCtePayload>,
    ) -> Result<()> {
        let local_file_size = data_size.max(self.spill_unit_size()).max(1);
        let mut writer = writer_creator.open(Some(local_file_size))?;
        let mut row_groups = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            row_groups.push(writer.add_row_group(vec![data_block])?);
        }
        let reader = writer.close()?;

        payloads.extend(row_groups.into_iter().map(|row_group| {
            MaterializedCtePayload::Spilled(MaterializedCteSpilledPayload {
                reader: reader.clone(),
                row_group,
            })
        }));
        Ok(())
    }

    fn consume_block(&mut self, data_block: DataBlock) -> Result<()> {
        let can_spill_block = data_block.num_columns() > 0 && !data_block.is_empty();
        if can_spill_block && self.memory_settings.check_spill() {
            self.spilling_bytes += data_block.memory_size();
            self.spilling_blocks.push(data_block);

            if self.spilling_bytes >= self.spill_unit_size()
                || self.spilling_blocks.len() >= SpillWriter::MAX_ROW_GROUPS_PER_FILE
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

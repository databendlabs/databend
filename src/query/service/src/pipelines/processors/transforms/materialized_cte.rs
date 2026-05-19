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

use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;
use databend_storages_common_cache::TempDirManager;

use crate::sessions::QueryContext;
use crate::sessions::TableContextQueryIdentity;
use crate::spillers::Location;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerInner;
use crate::spillers::SpillerType;

pub type MaterializedCteSpiller = SpillerInner<Arc<QueryContext>>;

#[derive(Clone)]
pub enum MaterializedCtePayload {
    InMemory(DataBlock),
    Spilled(Location),
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
    SpillerInner::new(ctx, operator, config)
}

pub struct MaterializedCteSink {
    senders: Vec<Sender<MaterializedCtePayload>>,
    spiller: MaterializedCteSpiller,
    memory_settings: MemorySettings,
}

impl MaterializedCteSink {
    pub fn create(
        input: Arc<InputPort>,
        senders: Vec<Sender<MaterializedCtePayload>>,
        spiller: MaterializedCteSpiller,
        memory_settings: MemorySettings,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            senders,
            spiller,
            memory_settings,
        })))
    }
}

#[async_trait::async_trait]
impl AsyncSink for MaterializedCteSink {
    const NAME: &'static str = "MaterializedCteSink";

    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let payload = if self.memory_settings.check_spill() {
            let location = self.spiller.spill(vec![data_block]).await?;
            MaterializedCtePayload::Spilled(location)
        } else {
            MaterializedCtePayload::InMemory(data_block)
        };

        for sender in self.senders.iter() {
            sender.send(payload.clone()).await.map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        for sender in self.senders.iter() {
            sender.close();
        }
        Ok(())
    }
}

pub struct CTESource {
    receiver: Receiver<MaterializedCtePayload>,
    spiller: MaterializedCteSpiller,
}

impl CTESource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        receiver: Receiver<MaterializedCtePayload>,
        spiller: MaterializedCteSpiller,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output_port, Self {
            receiver,
            spiller,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for CTESource {
    const NAME: &'static str = "MaterializeCTESource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Ok(payload) = self.receiver.recv().await {
            let data = match payload {
                MaterializedCtePayload::InMemory(data) => data,
                MaterializedCtePayload::Spilled(location) => {
                    self.spiller.read_spilled_file(&location).await?
                }
            };
            return Ok(Some(data));
        }
        Ok(None)
    }
}

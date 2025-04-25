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
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::RemoteExpr;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

pub struct RuntimeFilterSourceProcessor {
    pub meta_receiver: Receiver<RuntimeFiltersMeta>,
}

impl RuntimeFilterSourceProcessor {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<RuntimeFiltersMeta>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self {
            meta_receiver: receiver,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for RuntimeFilterSourceProcessor {
    const NAME: &'static str = "RuntimeFilterSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let start = std::time::Instant::now();
        log::info!("RuntimeFilterSource recv() start");
        let rf = self.meta_receiver.recv().await;
        log::info!(
            "RuntimeFilterSource recv() take {:?},get {}",
            start.elapsed(),
            rf.is_ok()
        );
        match rf {
            Ok(runtime_filter) => Ok(Some(DataBlock::empty_with_meta(Box::new(runtime_filter)))),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RuntimeFiltersMeta {
    runtime_filters: Vec<RuntimeFilterMeta>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterMeta {
    scan_id: usize,
    inlist: Vec<RemoteExpr<String>>,
    min_max: Vec<RemoteExpr<String>>,
}

#[typetag::serde(name = "runtime_filters_meta")]
impl BlockMetaInfo for RuntimeFiltersMeta {
    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl RuntimeFiltersMeta {
    pub fn add(&mut self, scan_id: usize, runtime_filter_info: &RuntimeFilterInfo) {
        let rf = RuntimeFilterMeta {
            scan_id,
            inlist: runtime_filter_info
                .inlists_ref()
                .iter()
                .map(|expr| expr.as_remote_expr())
                .collect(),
            min_max: runtime_filter_info
                .min_maxs_ref()
                .iter()
                .map(|expr| expr.as_remote_expr())
                .collect(),
        };
        self.runtime_filters.push(rf);
    }
}
pub struct RuntimeFilterSinkProcessor {
    node_num: usize,
    recv_num: usize,
}

impl RuntimeFilterSinkProcessor {
    pub fn create(input: Arc<InputPort>, node_num: usize) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            node_num,
            recv_num: 0,
        })))
    }
}

impl RuntimeFilterSinkProcessor {}

#[async_trait::async_trait]
impl AsyncSink for RuntimeFilterSinkProcessor {
    const NAME: &'static str = "RuntimeFilterSink";

    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let ptr = data_block
            .take_meta()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to RuntimeFilterMeta"))?;
        let runtime_filter = RuntimeFiltersMeta::downcast_from(ptr)
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to RuntimeFilterMeta"))?;
        log::info!(
            "RuntimeFilterSinkProcessor recv runtime filter: {:?}",
            runtime_filter
        );
        self.recv_num += 1;
        Ok(self.node_num == self.recv_num)
    }
}

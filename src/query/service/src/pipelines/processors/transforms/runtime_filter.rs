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
    pub meta_receiver: Receiver<RuntimeFilterMeta>,
}

impl RuntimeFilterSourceProcessor {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<RuntimeFilterMeta>,
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
        match self.meta_receiver.recv().await {
            Ok(runtime_filter) => Ok(Some(DataBlock::empty_with_meta(Box::new(
                RuntimeFilterMeta {
                    inlist: runtime_filter.inlist,
                    min_max: runtime_filter.min_max,
                },
            )))),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterMeta {
    inlist: Vec<RemoteExpr<String>>,
    min_max: Vec<RemoteExpr<String>>,
}

#[typetag::serde(name = "runtime_filter_meta")]
impl BlockMetaInfo for RuntimeFilterMeta {
    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl From<&RuntimeFilterInfo> for RuntimeFilterMeta {
    fn from(runtime_filter_info: &RuntimeFilterInfo) -> Self {
        Self {
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
        }
    }
}
pub struct RuntimeFilterSinkProcessor {}

impl RuntimeFilterSinkProcessor {
    pub fn create(input: Arc<InputPort>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {})))
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
        let runtime_filter = RuntimeFilterMeta::downcast_from(ptr)
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to RuntimeFilterMeta"))?;
        log::info!(
            "RuntimeFilterSinkProcessor recv runtime filter: {:?}",
            runtime_filter
        );
        Ok(true)
    }
}

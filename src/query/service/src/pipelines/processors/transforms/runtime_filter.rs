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

use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_catalog::runtime_filter_info::RuntimeFiltersForScan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

pub struct RuntimeFilterSourceProcessor {
    pub receiver: Receiver<RemoteRuntimeFilters>,
}

impl RuntimeFilterSourceProcessor {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<RemoteRuntimeFilters>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self { receiver })
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
        let rf = self.receiver.recv().await;
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

pub struct RuntimeFilterSinkProcessor {
    node_num: usize,
    recv_num: usize,
    rf: Vec<RemoteRuntimeFilters>,
    sender: Sender<RemoteRuntimeFilters>,
}

impl RuntimeFilterSinkProcessor {
    pub fn create(
        input: Arc<InputPort>,
        node_num: usize,
        sender: Sender<RemoteRuntimeFilters>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            node_num,
            recv_num: 0,
            rf: vec![],
            sender,
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
        let runtime_filter = RemoteRuntimeFilters::downcast_from(ptr)
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to RuntimeFilterMeta"))?;
        log::info!(
            "RuntimeFilterSinkProcessor recv runtime filter: {:?}",
            runtime_filter
        );
        self.recv_num += 1;
        self.rf.push(runtime_filter);
        let all_recv = self.node_num == self.recv_num;
        if all_recv {
            let merged_rf = RemoteRuntimeFilters::merge(&self.rf);
            self.sender.send(merged_rf).await.map_err(|_| {
                ErrorCode::Internal("RuntimeFilterSinkProcessor failed to send runtime filter")
            })?;
        }
        Ok(all_recv)
    }
}

/// One-to-one correspondence with HashJoin operator.
///
/// When the build side is empty, `scan_id_to_runtime_filter` is `None`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RemoteRuntimeFilters {
    scan_id_to_runtime_filter: Option<HashMap<usize, RemoteRuntimeFiltersForScan>>,
}

impl From<Option<HashMap<usize, RuntimeFiltersForScan>>> for RemoteRuntimeFilters {
    fn from(rfs: Option<HashMap<usize, RuntimeFiltersForScan>>) -> Self {
        RemoteRuntimeFilters {
            scan_id_to_runtime_filter: rfs.map(|rfs| {
                rfs.into_iter()
                    .map(|(scan_id, runtime_filter)| (scan_id, runtime_filter.into()))
                    .collect()
            }),
        }
    }
}

impl From<RemoteRuntimeFilters> for Option<HashMap<usize, RuntimeFiltersForScan>> {
    fn from(rfs: RemoteRuntimeFilters) -> Self {
        rfs.scan_id_to_runtime_filter.map(|rfs| {
            rfs.into_iter()
                .map(|(scan_id, runtime_filter)| (scan_id, runtime_filter.into()))
                .collect()
        })
    }
}

impl From<RemoteRuntimeFiltersForScan> for RuntimeFiltersForScan {
    fn from(rfs: RemoteRuntimeFiltersForScan) -> Self {
        Self {
            inlist: rfs
                .rf_id_to_inlist
                .into_iter()
                .map(|(id, expr)| (id, expr.as_expr(&BUILTIN_FUNCTIONS)))
                .collect(),
            min_max: rfs
                .rf_id_to_min_max
                .into_iter()
                .map(|(id, expr)| (id, expr.as_expr(&BUILTIN_FUNCTIONS)))
                .collect(),
            bloom: Default::default(),
        }
    }
}

impl From<RuntimeFiltersForScan> for RemoteRuntimeFiltersForScan {
    fn from(rfs: RuntimeFiltersForScan) -> Self {
        Self {
            rf_id_to_inlist: rfs
                .inlist
                .iter()
                .map(|(id, expr)| (*id, expr.as_remote_expr()))
                .collect(),
            rf_id_to_min_max: rfs
                .min_max
                .iter()
                .map(|(id, expr)| (*id, expr.as_remote_expr()))
                .collect(),
        }
    }
}
impl RemoteRuntimeFilters {
    pub fn merge(rfs: &[RemoteRuntimeFilters]) -> Self {
        log::info!("start merge runtime filters: {:?}", rfs);
        let rfs = rfs
            .iter()
            .filter_map(|rfs| rfs.scan_id_to_runtime_filter.as_ref())
            .collect::<Vec<_>>();

        if rfs.is_empty() {
            return RemoteRuntimeFilters::default();
        }

        let mut common_scans: Vec<usize> = rfs[0].keys().cloned().collect();
        for rf in &rfs[1..] {
            common_scans.retain(|scan_id| rf.contains_key(scan_id));
        }

        let mut merged = HashMap::new();

        for scan_id in common_scans {
            let mut merged_for_scan = RemoteRuntimeFiltersForScan::default();
            let first_scan = &rfs[0][&scan_id];

            let mut common_inlist_ids: Vec<usize> =
                first_scan.rf_id_to_inlist.keys().cloned().collect();
            let mut common_min_max_ids: Vec<usize> =
                first_scan.rf_id_to_min_max.keys().cloned().collect();
            for rf in &rfs[1..] {
                let scan_filter = &rf[&scan_id];
                common_inlist_ids.retain(|id| scan_filter.rf_id_to_inlist.contains_key(id));
                common_min_max_ids.retain(|id| scan_filter.rf_id_to_min_max.contains_key(id));
            }

            for rf_id in &common_inlist_ids {
                let mut exprs = Vec::new();
                for rf in rfs.iter() {
                    exprs.push(rf[&scan_id].rf_id_to_inlist[rf_id].clone());
                }
                log::info!("merge inlist: {:?}, rf_id: {:?}", exprs, rf_id);
                let merged_expr = exprs
                    .into_iter()
                    .reduce(|acc, expr| RemoteExpr::FunctionCall {
                        span: None,
                        id: Box::new(FunctionID::Builtin {
                            name: "or".to_string(),
                            id: 1,
                        }),
                        generics: vec![],
                        args: vec![acc, expr],
                        return_type: DataType::Nullable(Box::new(DataType::Boolean)),
                    })
                    .unwrap();
                merged_for_scan.rf_id_to_inlist.insert(*rf_id, merged_expr);
            }

            for rf_id in &common_min_max_ids {
                let mut exprs = Vec::new();
                for rf in rfs.iter() {
                    exprs.push(rf[&scan_id].rf_id_to_min_max[rf_id].clone());
                }
                log::info!("merge min_max: {:?}, rf_id: {:?}", exprs, rf_id);
                let merged_expr = exprs
                    .into_iter()
                    .reduce(|acc, expr| RemoteExpr::FunctionCall {
                        span: None,
                        id: Box::new(FunctionID::Builtin {
                            name: "or".to_string(),
                            id: 1,
                        }),
                        generics: vec![],
                        args: vec![acc, expr],
                        return_type: DataType::Nullable(Box::new(DataType::Boolean)),
                    })
                    .unwrap();
                merged_for_scan.rf_id_to_min_max.insert(*rf_id, merged_expr);
            }

            merged.insert(scan_id, merged_for_scan);
        }

        RemoteRuntimeFilters {
            scan_id_to_runtime_filter: Some(merged),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RemoteRuntimeFiltersForScan {
    rf_id_to_inlist: HashMap<usize, RemoteExpr<String>>,
    rf_id_to_min_max: HashMap<usize, RemoteExpr<String>>,
}

#[typetag::serde(name = "runtime_filters_for_join")]
impl BlockMetaInfo for RemoteRuntimeFilters {
    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

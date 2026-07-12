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

use databend_common_catalog::plan::ReclusterDepthKind;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;

use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::recluster::ReclusterDepthStats;
use crate::operations::recluster::calculate_max_depth;
use crate::operations::recluster::collect_depth_stats;

/// Emits recluster progress metadata by comparing output depth with the selected task depth.
pub struct TransformReclusterDepth {
    kind: ReclusterDepthKind,
    before_max_depth: usize,
    output_stats: Vec<ReclusterDepthStats>,
}

impl TransformReclusterDepth {
    pub fn new(kind: ReclusterDepthKind, before_max_depth: usize) -> Self {
        Self {
            kind,
            before_max_depth,
            output_stats: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TransformReclusterDepth {
    const NAME: &'static str = "TransformReclusterDepth";

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let logs = MutationLogs::try_from(data)?;
        for entry in &logs.entries {
            if let MutationLogEntry::AppendBlock { block_meta, .. } = entry {
                let block = &block_meta.block_meta;
                let cluster_stats = block.cluster_stats.clone().ok_or_else(|| {
                    ErrorCode::Internal("recluster depth requires output cluster statistics")
                })?;
                self.output_stats
                    .push(collect_depth_stats(&self.kind, block, cluster_stats));
            }
        }
        Ok(Some(logs.into()))
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        if self.output_stats.is_empty() {
            return Err(ErrorCode::Internal(
                "recluster depth requires at least one output block",
            ));
        }
        let after_max_depth = calculate_max_depth(&self.kind, &self.output_stats)?;
        let improved_enough = after_max_depth.saturating_mul(2) <= self.before_max_depth;
        let logs = MutationLogs {
            entries: vec![MutationLogEntry::ReclusterExtras { improved_enough }],
        };
        Ok(Some(logs.into()))
    }
}

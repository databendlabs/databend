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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::FuseLazyPartInfo;
use crate::operations::analyze::AnalyzeAccumulator;
use crate::operations::analyze::SegmentAnalyzer;

/// Shared progress reporting for all collect sources of one ANALYZE run.
pub struct AnalyzeSegmentProgress {
    processed: AtomicUsize,
    total: usize,
    stride: usize,
    started_at: Instant,
}

impl AnalyzeSegmentProgress {
    pub fn new(total: usize, max_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            processed: AtomicUsize::new(0),
            total,
            stride: std::cmp::max(max_threads * 4, 1),
            started_at: Instant::now(),
        })
    }

    fn record(&self, ctx: &dyn TableContext) {
        let processed = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        if processed == self.total || processed.is_multiple_of(self.stride) {
            ctx.set_status_info(&format!(
                "analyze: read segment files:{}/{}, cost:{:?}",
                processed,
                self.total,
                self.started_at.elapsed()
            ));
        }
    }
}

/// Drives a [`SegmentAnalyzer`] over the partitions assigned to this source and ships the
/// accumulated statistics to the sink once.
pub struct AnalyzeCollectSource {
    output: Arc<OutputPort>,
    ctx: Arc<dyn TableContext>,
    analyzer: Arc<SegmentAnalyzer>,
    progress: Arc<AnalyzeSegmentProgress>,
    acc: Option<AnalyzeAccumulator>,
    pending: Option<PartInfoPtr>,
}

impl AnalyzeCollectSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        ctx: Arc<dyn TableContext>,
        analyzer: Arc<SegmentAnalyzer>,
        progress: Arc<AnalyzeSegmentProgress>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            ctx,
            analyzer,
            progress,
            acc: Some(AnalyzeAccumulator::default()),
            pending: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for AnalyzeCollectSource {
    fn name(&self) -> String {
        "AnalyzeCollectSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.acc.is_none() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.pending.is_none() {
            match self.ctx.get_partition() {
                Some(part) => self.pending = Some(part),
                None => {
                    let acc = self.acc.take().unwrap();
                    self.output
                        .push_data(Ok(DataBlock::empty_with_meta(Box::new(acc))));
                    return Ok(Event::NeedConsume);
                }
            }
        }
        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let Some(part) = self.pending.take() else {
            return Ok(());
        };
        let part = FuseLazyPartInfo::from_part(&part)?;
        let acc = self.acc.as_mut().unwrap();
        self.analyzer.analyze(&part.segment_location, acc).await?;
        self.progress.record(self.ctx.as_ref());
        Ok(())
    }
}

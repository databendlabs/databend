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
use std::mem;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::mpsc::Receiver;
use databend_common_base::base::ProgressValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::processors::Profile;
use databend_common_pipeline_core::processors::ProfileStatisticsName;
use log::debug;

use crate::input_formats::input_pipeline::AligningStateTrait;
use crate::input_formats::input_pipeline::InputFormatPipe;
use crate::input_formats::input_pipeline::ReadBatchTrait;
use crate::input_formats::input_pipeline::RowBatchTrait;
use crate::input_formats::input_pipeline::Split;
use crate::input_formats::InputContext;

pub struct Aligner<I: InputFormatPipe> {
    ctx: Arc<InputContext>,
    output: Arc<OutputPort>,

    // input
    split_rx: async_channel::Receiver<Result<Split<I>>>,

    state: Option<I::AligningState>,
    batch_rx: Option<Receiver<Result<I::ReadBatch>>>,
    read_batch: Option<I::ReadBatch>,

    is_flushing_split: bool,
    no_more_split: bool,

    // output
    row_batches: VecDeque<I::RowBatch>,
}

impl<I: InputFormatPipe> Aligner<I> {
    pub(crate) fn try_create(
        output: Arc<OutputPort>,
        ctx: Arc<InputContext>,
        split_rx: async_channel::Receiver<Result<Split<I>>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            ctx,
            output,
            split_rx,
            state: None,
            read_batch: None,
            batch_rx: None,
            is_flushing_split: false,
            no_more_split: false,
            row_batches: Default::default(),
        })))
    }
}

#[async_trait::async_trait]
impl<I: InputFormatPipe> Processor for Aligner<I> {
    fn name(&self) -> String {
        "Aligner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            Ok(Event::Finished)
        } else if self.no_more_split && self.row_batches.is_empty() && self.read_batch.is_none() {
            self.output.finish();
            Ok(Event::Finished)
        } else if !self.output.can_push() {
            Ok(Event::NeedConsume)
        } else if let Some(rb) = self.row_batches.pop_front() {
            let block = DataBlock::empty_with_meta(Box::new(rb));
            self.output.push_data(Ok(block));
            Ok(Event::NeedConsume)
        } else if self.read_batch.is_some() || self.is_flushing_split {
            Ok(Event::Sync)
        } else {
            Ok(Event::Async)
        }
    }

    fn process(&mut self) -> Result<()> {
        match &mut self.state {
            Some(state) => {
                let mut process_values = ProgressValues { rows: 0, bytes: 0 };
                let read_batch = mem::take(&mut self.read_batch);
                process_values.bytes += read_batch.as_ref().map(|b| b.size()).unwrap_or_default();
                let eof = read_batch.is_none();
                let row_batches = state.align(read_batch)?;
                for b in row_batches.into_iter() {
                    if b.rows() > 0 || b.size() > 0 {
                        process_values.rows += b.rows();
                        self.row_batches.push_back(b);
                    }
                }
                if eof {
                    assert!(self.is_flushing_split);
                }
                if self.is_flushing_split {
                    if !eof {
                        // just aligned data beyond end
                        let row_batches = state.align(None)?;
                        for b in row_batches.into_iter() {
                            self.row_batches.push_back(b);
                        }
                    }
                    self.is_flushing_split = false;
                    self.state = None;
                    self.batch_rx = None;
                }
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytes,
                    process_values.bytes,
                );
                self.ctx.scan_progress.incr(&process_values);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("Aligner process state is none")),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if !self.no_more_split {
            match &self.state {
                None => match self.split_rx.recv().await {
                    Ok(Ok(split)) => {
                        self.state = Some(I::try_create_align_state(&self.ctx, &split.info)?);
                        self.batch_rx = Some(split.rx);
                        debug!("aligner recv new split {}", &split.info);
                    }
                    Ok(Err(e)) => {
                        return Err(e);
                    }
                    Err(_) => {
                        debug!("aligner no more split");
                        self.no_more_split = true;
                    }
                },
                Some(state) => {
                    if let Some(rx) = self.batch_rx.as_mut() {
                        match rx.recv().await {
                            Some(Ok(batch)) => {
                                debug!("aligner recv new batch");
                                self.read_batch = Some(batch)
                            }
                            Some(Err(e)) => {
                                return Err(e);
                            }
                            None => {
                                debug!("aligner recv end of current split");
                                if let Some(reader) = state.read_beyond_end() {
                                    let end = reader.read().await?;
                                    if !end.is_empty() {
                                        debug!("aligner read {} bytes beyond end", end.len());
                                        let batch = I::ReadBatch::from(end);
                                        self.read_batch = Some(batch);
                                    }
                                }
                                self.is_flushing_split = true;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

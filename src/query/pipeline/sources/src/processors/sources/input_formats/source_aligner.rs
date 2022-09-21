//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use common_base::base::tokio::sync::mpsc::Receiver;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use crossbeam_channel::TrySendError;

use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_pipeline::Split;

pub struct Aligner<I: InputFormatPipe> {
    ctx: Arc<InputContext>,
    output: Arc<OutputPort>,

    // input
    split_rx: async_channel::Receiver<Split<I>>,

    state: Option<I::AligningState>,
    batch_rx: Option<Receiver<Result<I::ReadBatch>>>,
    read_batch: Option<I::ReadBatch>,

    received_end_batch_of_split: bool,
    no_more_split: bool,

    // output
    row_batches: VecDeque<I::RowBatch>,
    row_batch_tx: crossbeam_channel::Sender<I::RowBatch>,
}

impl<I: InputFormatPipe> Aligner<I> {
    pub(crate) fn try_create(
        output: Arc<OutputPort>,
        ctx: Arc<InputContext>,
        split_rx: async_channel::Receiver<Split<I>>,
        batch_tx: crossbeam_channel::Sender<I::RowBatch>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            ctx,
            output,
            split_rx,
            row_batch_tx: batch_tx,
            state: None,
            read_batch: None,
            batch_rx: None,
            received_end_batch_of_split: false,
            no_more_split: false,
            row_batches: Default::default(),
        })))
    }
}

#[async_trait::async_trait]
impl<I: InputFormatPipe> Processor for Aligner<I> {
    fn name(&self) -> &'static str {
        "Aligner"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.no_more_split && self.row_batches.is_empty() && self.read_batch.is_none() {
            self.output.finish();
            Ok(Event::Finished)
        } else if let Some(rb) = self.row_batches.pop_front() {
            match self.row_batch_tx.try_send(rb) {
                Ok(()) => {
                    tracing::debug!("aligner send row batch ok");
                    self.output.push_data(Err(ErrorCode::Ok("")));
                    Ok(Event::NeedConsume)
                }
                Err(TrySendError::Full(b)) => {
                    tracing::debug!("aligner send row batch full");
                    self.row_batches.push_front(b);
                    Ok(Event::NeedConsume)
                }
                Err(TrySendError::Disconnected(_)) => {
                    tracing::debug!("aligner send row batch disconnected");
                    self.output.finish();
                    Ok(Event::Finished)
                }
            }
        } else if self.read_batch.is_some() || self.received_end_batch_of_split {
            Ok(Event::Sync)
        } else {
            Ok(Event::Async)
        }
    }

    fn process(&mut self) -> Result<()> {
        match &mut self.state {
            Some(state) => {
                let read_batch = mem::take(&mut self.read_batch);
                let eof = read_batch.is_none();
                let row_batches = state.align(read_batch)?;
                for b in row_batches.into_iter() {
                    self.row_batches.push_back(b);
                }
                if eof {
                    self.state = None;
                    self.batch_rx = None;
                }
                self.received_end_batch_of_split = false;
                Ok(())
            }
            _ => Err(ErrorCode::UnexpectedError("Aligner process state is none")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.no_more_split {
            if self.state.is_none() {
                match self.split_rx.recv().await {
                    Ok(split) => {
                        self.state = Some(I::AligningState::try_create(&self.ctx, &split.info)?);
                        self.batch_rx = Some(split.rx);
                        self.received_end_batch_of_split = false;
                        tracing::debug!(
                            "aligner recv new split {} {}",
                            &split.info.file_info.path,
                            split.info.seq_infile
                        );
                    }
                    Err(_) => {
                        tracing::debug!("aligner no more split");
                        self.no_more_split = true;
                    }
                }
            }
            if let Some(rx) = self.batch_rx.as_mut() {
                match rx.recv().await {
                    Some(Ok(batch)) => {
                        tracing::debug!("aligner recv new batch");
                        self.read_batch = Some(batch)
                    }
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    None => {
                        tracing::debug!("aligner recv end of current split");
                        self.received_end_batch_of_split = true;
                    }
                }
            }
        }
        Ok(())
    }
}

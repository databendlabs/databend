// Copyright 2022 Datafuse Labs.
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

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_exception::Result;
use common_formats::InputFormat;
use opendal::io_util::CompressAlgorithm;
use opendal::Operator;
use parking_lot::Mutex;

use super::file_splitter::FileSplitter;
use super::file_splitter::State;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;

pub struct MultiFileSplitter {
    finished: bool,
    storage_operator: Operator,
    current_file: Option<FileSplitter>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    output_splits: VecDeque<Vec<u8>>,
    compress_algorithm: Option<CompressAlgorithm>,
    input_format: Arc<dyn InputFormat>,
    files: Arc<Mutex<VecDeque<String>>>,
}

impl MultiFileSplitter {
    pub fn create(
        storage_operator: Operator,
        scan_progress: Arc<Progress>,
        output: Arc<OutputPort>,
        input_format: Arc<dyn InputFormat>,
        compress_algorithm: Option<CompressAlgorithm>,
        files: Arc<Mutex<VecDeque<String>>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MultiFileSplitter {
            storage_operator,
            current_file: None,
            output,
            scan_progress,
            output_splits: Default::default(),
            compress_algorithm,
            input_format,
            finished: false,
            files,
        })))
    }

    pub fn next_file(&mut self) -> Option<String> {
        let mut files_guard = self.files.lock();
        files_guard.pop_front()
    }

    pub async fn init_file(&mut self, path: &str) -> Result<()> {
        let object = self.storage_operator.object(&path);
        let reader = object.reader().await?;
        self.current_file = Some(FileSplitter::create(
            reader,
            self.input_format.clone(),
            self.compress_algorithm,
        ));
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for MultiFileSplitter {
    fn name(&self) -> &'static str {
        "MultiFileSplitter"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(split) = self.output_splits.pop_front() {
            self.output.push_buf(Ok(split));
            return Ok(Event::NeedConsume);
        }

        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        match &self.current_file {
            None => Ok(Event::Async),
            Some(splitter) => match &splitter.state() {
                State::NeedData => Ok(Event::Async),
                State::Finished => Ok(Event::Async),
                State::ReceivedData(_) => Ok(Event::Sync),
                State::NeedFlush => Ok(Event::Sync),
            },
        }
    }

    fn process(&mut self) -> Result<()> {
        let progress_values = ProgressValues::default();
        let current = self.current_file.as_mut().unwrap();
        if matches!(current.state(), State::Finished) {
            self.current_file = None;
        }
        self.scan_progress.incr(&progress_values);
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if self.current_file.is_none() {
            match self.next_file() {
                None => {
                    self.finished = true;
                    return Ok(());
                }
                Some(path) => {
                    self.init_file(&path).await?;
                }
            }
        }
        let current = self.current_file.as_mut().unwrap();
        current.async_process();
        Ok(())
    }
}

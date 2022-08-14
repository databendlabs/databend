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
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::InputFormat;
use common_io::prelude::FileSplit;
use common_io::prelude::FormatSettings;
use common_meta_types::StageFileCompression;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_storage::init_operator;
use common_storage::StorageParams;
use opendal::io_util::CompressAlgorithm;
use opendal::Operator;
use parking_lot::Mutex;

use super::file_splitter::FileSplitter;
use super::file_splitter::FileSplitterState;

pub struct MultiFileSplitter {
    finished: bool,
    storage_operator: OperatorInfo,
    current_file: Option<FileSplitter>,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,
    output_splits: VecDeque<FileSplit>,
    input_format: Arc<dyn InputFormat>,
    format_settings: FormatSettings,
    files: Arc<Mutex<VecDeque<String>>>,
    compress_option: StageFileCompression,
}

#[derive(Clone)]
pub enum OperatorInfo {
    Op(Operator),
    Cfg(StorageParams),
}

impl MultiFileSplitter {
    pub fn create(
        storage_operator: OperatorInfo,
        scan_progress: Arc<Progress>,
        output: Arc<OutputPort>,
        input_format: Arc<dyn InputFormat>,
        compress_option: StageFileCompression,
        format_settings: FormatSettings,
        files: Arc<Mutex<VecDeque<String>>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MultiFileSplitter {
            storage_operator,
            current_file: None,
            output,
            scan_progress,
            output_splits: Default::default(),
            compress_option,
            input_format,
            format_settings,
            finished: false,
            files,
        })))
    }

    pub fn next_file(&mut self) -> Option<String> {
        let mut files_guard = self.files.lock();
        files_guard.pop_front()
    }

    pub async fn init_file(&mut self, path: &str) -> Result<()> {
        let op = match &self.storage_operator {
            OperatorInfo::Op(op) => op.clone(),
            OperatorInfo::Cfg(cfg) => {
                let op = init_operator(cfg)?;
                self.storage_operator = OperatorInfo::Op(op.clone());
                op
            }
        };
        let object = op.object(path);
        let reader = object.reader().await?;
        self.current_file = Some(FileSplitter::create(
            reader,
            Some(path.to_string()),
            self.input_format.clone(),
            self.format_settings.clone(),
            self.get_compression_algo(path)?,
        ));
        Ok(())
    }

    fn get_compression_algo(&self, path: &str) -> Result<Option<CompressAlgorithm>> {
        let compression_algo = match self.compress_option {
            StageFileCompression::Auto => CompressAlgorithm::from_path(path),
            StageFileCompression::Gzip => Some(CompressAlgorithm::Gzip),
            StageFileCompression::Bz2 => Some(CompressAlgorithm::Bz2),
            StageFileCompression::Brotli => Some(CompressAlgorithm::Brotli),
            StageFileCompression::Zstd => Some(CompressAlgorithm::Zstd),
            StageFileCompression::Deflate => Some(CompressAlgorithm::Zlib),
            StageFileCompression::RawDeflate => Some(CompressAlgorithm::Deflate),
            StageFileCompression::Xz => Some(CompressAlgorithm::Xz),
            StageFileCompression::Lzo => {
                return Err(ErrorCode::UnImplement("compress type lzo is unimplemented"));
            }
            StageFileCompression::Snappy => {
                return Err(ErrorCode::UnImplement(
                    "compress type snappy is unimplemented",
                ));
            }
            StageFileCompression::None => None,
        };
        Ok(compression_algo)
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
            self.output.push_split(Ok(split));
            return Ok(Event::NeedConsume);
        }

        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        match &self.current_file {
            None => Ok(Event::Async),
            Some(splitter) => match &splitter.state() {
                FileSplitterState::NeedData => Ok(Event::Async),
                FileSplitterState::Finished => Ok(Event::Async),
                FileSplitterState::ReceivedData(_) => Ok(Event::Sync),
                FileSplitterState::NeedFlush => Ok(Event::Sync),
            },
        }
    }

    fn process(&mut self) -> Result<()> {
        let mut progress_values = ProgressValues::default();
        let current = self.current_file.as_mut().unwrap();
        let mut output_splits = VecDeque::default();
        current.process(&mut output_splits, &mut progress_values)?;
        if matches!(current.state(), FileSplitterState::Finished) {
            self.current_file = None;
        }
        self.output_splits.extend(output_splits.into_iter());
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
        current.async_process().await?;
        Ok(())
    }
}

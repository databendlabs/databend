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
use std::cmp::min;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use opendal::io_util::CompressAlgorithm;
use opendal::Object;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::input_context::InputContext;

pub trait InputData: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

pub trait InputState: Send {
    fn as_any(&mut self) -> &mut dyn Any;
}

#[async_trait::async_trait]
pub trait InputFormat: Send + Sync {
    fn get_format_settings(&self, settings: &Arc<Settings>) -> Result<FormatSettings>;

    fn default_record_delimiter(&self) -> RecordDelimiter;

    fn default_field_delimiter(&self) -> u8;

    async fn read_file_meta(&self, obj: &Object, size: usize)
    -> Result<Option<Arc<dyn InputData>>>;

    async fn read_split_meta(
        &self,
        obj: &Object,
        split_info: &SplitInfo,
    ) -> Result<Option<Box<dyn InputData>>>;

    fn split_files(&self, file_infos: Vec<FileInfo>, split_size: usize) -> Vec<SplitInfo>;

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()>;

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()>;
}

#[derive(Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: usize,
    pub compress_alg: Option<CompressAlgorithm>,
    pub file_meta: Option<Arc<dyn InputData>>,
}

impl FileInfo {
    pub fn split_by_size(&self, split_size: usize) -> Vec<SplitInfo> {
        let mut splits = vec![];
        let n = (self.size + split_size - 1) / split_size;
        for i in 0..n - 1 {
            splits.push(SplitInfo {
                file_info: self.clone(),
                seq_infile: i,
                is_end: i == n - 1,
                offset: i * split_size,
                len: min((i + 1) * split_size, self.size),
                split_meta: None,
            })
        }
        splits
    }
}

impl Debug for FileInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileInfo")
            .field("path", &self.path)
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Clone)]
pub struct SplitInfo {
    pub file_info: FileInfo,
    pub seq_infile: usize,
    pub is_end: bool,
    pub offset: usize,
    pub len: usize,
    pub split_meta: Option<Arc<dyn InputData + 'static>>,
}

impl SplitInfo {
    pub fn from_file_info(file_info: FileInfo) -> Self {
        let len = file_info.size;
        Self {
            file_info,
            seq_infile: 0,
            is_end: true,
            offset: 0,
            len,
            split_meta: None,
        }
    }

    pub fn from_stream_split(path: String, compress_alg: Option<CompressAlgorithm>) -> Self {
        SplitInfo {
            file_info: FileInfo {
                path,
                size: 0,
                compress_alg,
                file_meta: None,
            },
            seq_infile: 0,
            offset: 0,
            len: 0,
            is_end: false,
            split_meta: None,
        }
    }
}

impl Debug for SplitInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitInfo")
            .field("file_info", &self.file_info)
            .field("seq_infile", &self.seq_infile)
            .finish()
    }
}

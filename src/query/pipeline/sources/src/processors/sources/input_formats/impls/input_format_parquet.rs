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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::parquet::metadata::RowGroupMetaData;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use opendal::Object;

use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_format::FileInfo;
use crate::processors::sources::input_formats::input_format::InputData;
use crate::processors::sources::input_formats::input_format::SplitInfo;
use crate::processors::sources::input_formats::input_pipeline::AligningStateTrait;
use crate::processors::sources::input_formats::input_pipeline::BlockBuilderTrait;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_pipeline::StreamingReadBatch;
use crate::processors::sources::input_formats::InputFormat;

struct InputFormatParquet;

#[async_trait::async_trait]
impl InputFormat for InputFormatParquet {
    async fn read_file_meta(
        &self,
        obj: &Object,
        size: usize,
    ) -> Result<Option<Arc<dyn InputData>>> {
        todo!()
    }

    async fn read_split_meta(
        &self,
        obj: &Object,
        split_info: &SplitInfo,
    ) -> Result<Option<Box<dyn InputData>>> {
        todo!()
    }

    fn split_files(&self, file_infos: Vec<FileInfo>, split_size: usize) -> Vec<SplitInfo> {
        todo!()
    }

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        todo!()
    }

    fn exec_stream(
        &self,
        ctx: Arc<InputContext>,
        pipeline: &mut Pipeline,
        input: Receiver<StreamingReadBatch>,
    ) -> Result<()> {
        todo!()
    }
}

pub struct ParquetFormatPipe;

#[async_trait::async_trait]
impl InputFormatPipe for ParquetFormatPipe {
    type ReadBatch = ReadBatch;
    type RowBatch = RowGroupInMemory;
    type AligningState = AligningState;
    type BlockBuilder = ParquetBlockBuilder;
}

pub struct SplitMeta {
    row_groups: Vec<RowGroupMetaData>,
}

pub struct RowGroupInMemory {}

impl Debug for RowGroupInMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowGroupInMemory")
    }
}

#[derive(Debug)]
pub enum ReadBatch {
    Buffer(Vec<u8>),
    RowGroup(RowGroupInMemory),
}

impl From<Vec<u8>> for ReadBatch {
    fn from(v: Vec<u8>) -> Self {
        Self::Buffer(v)
    }
}

pub struct ParquetBlockBuilder {
    ctx: Arc<InputContext>,
}

impl BlockBuilderTrait for ParquetBlockBuilder {
    type Pipe = ParquetFormatPipe;

    fn create(ctx: Arc<InputContext>) -> Self {
        ParquetBlockBuilder { ctx }
    }

    fn deserialize(&mut self, batch: Option<RowGroupInMemory>) -> Result<Vec<DataBlock>> {
        todo!()
    }
}

pub struct AligningState {
    buffers: Vec<Vec<u8>>,
}

impl AligningStateTrait for AligningState {
    type Pipe = ParquetFormatPipe;

    fn try_create(ctx: &Arc<InputContext>, split_info: &SplitInfo) -> Result<Self> {
        todo!()
    }

    fn align(&mut self, read_batch: Option<ReadBatch>) -> Result<Vec<RowGroupInMemory>> {
        todo!()
    }
}

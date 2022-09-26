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

use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use opendal::io_util::CompressAlgorithm;
use opendal::Object;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_split::SplitInfo;

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

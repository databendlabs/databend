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
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::Progress;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
use common_settings::Settings;
use opendal::io_util::CompressAlgorithm;
use opendal::Operator;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::impls::input_format_csv::InputFormatCSV;
use crate::processors::sources::input_formats::impls::input_format_ndjson::InputFormatNDJson;
use crate::processors::sources::input_formats::impls::input_format_parquet::InputFormatParquet;
use crate::processors::sources::input_formats::impls::input_format_tsv::InputFormatTSV;
use crate::processors::sources::input_formats::input_format::FileInfo;
use crate::processors::sources::input_formats::input_format::SplitInfo;
use crate::processors::sources::input_formats::input_format_text::InputFormatText;
use crate::processors::sources::input_formats::input_pipeline::StreamingReadBatch;
use crate::processors::sources::input_formats::InputFormat;

const MIN_ROW_PER_BLOCK: usize = 800 * 1000;

#[derive(Debug)]
pub enum InputPlan {
    CopyInto(Box<CopyIntoPlan>),
    StreamingLoad(StreamPlan),
}

impl InputPlan {
    pub fn as_stream(&self) -> Result<&StreamPlan> {
        match self {
            InputPlan::StreamingLoad(p) => Ok(p),
            _ => Err(ErrorCode::UnexpectedError("expect StreamingLoad")),
        }
    }
}

#[derive(Debug)]
pub struct CopyIntoPlan {
    pub stage_info: UserStageInfo,
    pub files: Vec<String>,
}

#[derive(Debug)]
pub struct StreamPlan {
    pub is_multi_part: bool,
    pub compression: StageFileCompression,
}

pub enum InputSource {
    Operator(Operator),
    // need Mutex because Arc<InputContext> is immutable and mpsc receiver can not clone
    Stream(Mutex<Option<Receiver<Result<StreamingReadBatch>>>>),
}

impl InputSource {
    pub fn take_receiver(&self) -> Result<Receiver<Result<StreamingReadBatch>>> {
        match &self {
            InputSource::Operator(_) => Err(ErrorCode::UnexpectedError(
                "should not happen: copy with streaming source",
            )),
            InputSource::Stream(i) => {
                let mut guard = i.lock().expect("must success");
                let opt = &mut *guard;
                let r = mem::take(opt).expect("must success");
                Ok(r)
            }
        }
    }

    pub fn get_operator(&self) -> Result<Operator> {
        match self {
            InputSource::Operator(op) => Ok(op.clone()),
            InputSource::Stream(_) => Err(ErrorCode::UnexpectedError(
                "should not happen: copy with streaming source",
            )),
        }
    }
}

pub struct InputContext {
    pub plan: InputPlan,
    pub schema: DataSchemaRef,
    pub source: InputSource,
    pub format: Arc<dyn InputFormat>,
    pub splits: Vec<SplitInfo>,

    // row format only
    pub rows_to_skip: usize,
    pub field_delimiter: u8,
    pub record_delimiter: RecordDelimiter,

    // runtime config
    pub settings: Arc<Settings>,
    pub format_settings: FormatSettings,

    pub read_batch_size: usize,
    pub rows_per_block: usize,

    pub scan_progress: Arc<Progress>,
}

impl Debug for InputContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputContext")
            .field("plan", &self.plan)
            .field("rows_to_skip", &self.rows_to_skip)
            .field("field_delimiter", &self.field_delimiter)
            .field("record_delimiter", &self.record_delimiter)
            .field("format_settings", &self.format_settings)
            .field("rows_per_block", &self.rows_per_block)
            .field("read_batch_size", &self.read_batch_size)
            .field("num_splits", &self.splits.len())
            .finish()
    }
}

impl InputContext {
    pub fn get_input_format(format: &StageFileFormatType) -> Result<Arc<dyn InputFormat>> {
        match format {
            StageFileFormatType::Tsv => Ok(Arc::new(InputFormatText::<InputFormatTSV>::create())),
            StageFileFormatType::Csv => Ok(Arc::new(InputFormatText::<InputFormatCSV>::create())),
            StageFileFormatType::NdJson => {
                Ok(Arc::new(InputFormatText::<InputFormatNDJson>::create()))
            }
            StageFileFormatType::Parquet => Ok(Arc::new(InputFormatParquet {})),
            format => Err(ErrorCode::LogicalError(format!(
                "Unsupported file format: {:?}",
                format
            ))),
        }
    }

    pub async fn try_create_from_copy(
        operator: Operator,
        settings: Arc<Settings>,
        format_settings: FormatSettings,
        schema: DataSchemaRef,
        stage_info: UserStageInfo,
        files: Vec<String>,
        scan_progress: Arc<Progress>,
    ) -> Result<Self> {
        if files.is_empty() {
            return Err(ErrorCode::BadArguments("no file to copy"));
        }
        let plan = Box::new(CopyIntoPlan { stage_info, files });
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let split_size = 128usize * 1024 * 1024;
        let file_format_options = &plan.stage_info.file_format_options;
        let format = Self::get_input_format(&file_format_options.format)?;
        let file_infos = Self::get_file_infos(&format, &operator, &plan).await?;
        let splits = format.split_files(file_infos, split_size);
        let rows_per_block = MIN_ROW_PER_BLOCK;
        let record_delimiter = {
            if file_format_options.record_delimiter.is_empty() {
                format.default_record_delimiter()
            } else {
                RecordDelimiter::try_from(file_format_options.record_delimiter.as_str())?
            }
        };

        let rows_to_skip = file_format_options.skip_header as usize;
        let field_delimiter = {
            if file_format_options.field_delimiter.is_empty() {
                format.default_field_delimiter()
            } else {
                file_format_options.field_delimiter.as_bytes()[0]
            }
        };
        Ok(InputContext {
            format,
            schema,
            splits,
            settings,
            format_settings,
            record_delimiter,
            rows_per_block,
            read_batch_size,
            rows_to_skip,
            field_delimiter,
            scan_progress,
            source: InputSource::Operator(operator),
            plan: InputPlan::CopyInto(plan),
        })
    }

    pub async fn try_create_from_insert(
        format_name: &str,
        stream_receiver: Receiver<Result<StreamingReadBatch>>,
        settings: Arc<Settings>,
        schema: DataSchemaRef,
        scan_progress: Arc<Progress>,
        is_multi_part: bool,
    ) -> Result<Self> {
        let (format_name, rows_to_skip) = remove_clickhouse_format_suffix(format_name);
        let rows_to_skip = std::cmp::max(settings.get_skip_header()? as usize, rows_to_skip);

        let format_type =
            StageFileFormatType::from_str(format_name).map_err(ErrorCode::UnknownFormat)?;
        let format = Self::get_input_format(&format_type)?;
        let format_settings = format.get_format_settings(&settings)?;
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let rows_per_block = MIN_ROW_PER_BLOCK;
        let field_delimiter = settings.get_field_delimiter()?;
        let field_delimiter = {
            if field_delimiter.is_empty() {
                format.default_field_delimiter()
            } else {
                field_delimiter.as_bytes()[0]
            }
        };
        let record_delimiter = RecordDelimiter::try_from(&settings.get_record_delimiter()?[..])?;
        let compression = settings.get_compression()?;
        let compression = if !compression.is_empty() {
            StageFileCompression::from_str(&compression).map_err(ErrorCode::BadArguments)?
        } else {
            StageFileCompression::Auto
        };
        let plan = StreamPlan {
            is_multi_part,
            compression,
        };

        Ok(InputContext {
            format,
            schema,
            settings,
            format_settings,
            record_delimiter,
            rows_per_block,
            read_batch_size,
            field_delimiter,
            rows_to_skip,
            scan_progress,
            source: InputSource::Stream(Mutex::new(Some(stream_receiver))),
            plan: InputPlan::StreamingLoad(plan),
            splits: vec![],
        })
    }

    async fn get_file_infos(
        format: &Arc<dyn InputFormat>,
        op: &Operator,
        plan: &CopyIntoPlan,
    ) -> Result<Vec<FileInfo>> {
        let mut infos = vec![];
        for p in &plan.files {
            let obj = op.object(p);
            let size = obj.metadata().await?.content_length() as usize;
            let file_meta = format.read_file_meta(&obj, size).await?;
            let compress_alg = InputContext::get_compression_alg_copy(
                plan.stage_info.file_format_options.compression,
                p,
            )?;
            let info = FileInfo {
                path: p.clone(),
                size,
                compress_alg,
                file_meta,
            };
            infos.push(info)
        }
        Ok(infos)
    }

    pub fn num_prefetch_splits(&self) -> Result<usize> {
        Ok(self.settings.get_max_threads()? as usize)
    }

    pub fn num_prefetch_per_split(&self) -> usize {
        1
    }

    pub fn get_compression_alg(&self, path: &str) -> Result<Option<CompressAlgorithm>> {
        let opt = match &self.plan {
            InputPlan::CopyInto(p) => p.stage_info.file_format_options.compression,
            InputPlan::StreamingLoad(p) => p.compression,
        };
        Self::get_compression_alg_copy(opt, path)
    }

    pub fn get_compression_alg_copy(
        compress_option: StageFileCompression,
        path: &str,
    ) -> Result<Option<CompressAlgorithm>> {
        let compression_algo = match compress_option {
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

const WITH_NAMES_AND_TYPES: &str = "withnamesandtypes";
const WITH_NAMES: &str = "withnames";

fn remove_clickhouse_format_suffix(name: &str) -> (&str, usize) {
    let s = name.to_lowercase();
    let (suf_len, skip) = if s.ends_with(WITH_NAMES_AND_TYPES) {
        (WITH_NAMES_AND_TYPES.len(), 2)
    } else if s.ends_with(WITH_NAMES) {
        (WITH_NAMES.len(), 1)
    } else {
        (0, 0)
    };
    (&name[0..(s.len() - suf_len)], skip)
}

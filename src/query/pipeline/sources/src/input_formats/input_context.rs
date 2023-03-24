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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::Progress;
use common_compress::CompressAlgorithm;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataSchema;
use common_expression::TableSchemaRef;
use common_formats::ClickhouseFormatType;
use common_formats::FileFormatOptionsExt;
use common_meta_app::principal::FileFormatOptions;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageFileCompression;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::StageInfo;
use common_settings::Settings;
use dashmap::DashMap;
use opendal::Operator;

use crate::input_formats::impls::InputFormatCSV;
use crate::input_formats::impls::InputFormatNDJson;
use crate::input_formats::impls::InputFormatParquet;
use crate::input_formats::impls::InputFormatTSV;
use crate::input_formats::impls::InputFormatXML;
use crate::input_formats::InputError;
use crate::input_formats::InputFormat;
use crate::input_formats::SplitInfo;
use crate::input_formats::StreamingReadBatch;

#[derive(Debug)]
pub enum InputPlan {
    CopyInto(Box<CopyIntoPlan>),
    StreamingLoad(StreamPlan),
}

impl InputPlan {
    pub fn as_stream(&self) -> Result<&StreamPlan> {
        match self {
            InputPlan::StreamingLoad(p) => Ok(p),
            _ => Err(ErrorCode::Internal("expect StreamingLoad")),
        }
    }
}

#[derive(Debug)]
pub struct CopyIntoPlan {
    pub stage_info: StageInfo,
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
            InputSource::Operator(_) => Err(ErrorCode::Internal(
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
            InputSource::Stream(_) => Err(ErrorCode::Internal(
                "should not happen: copy with streaming source",
            )),
        }
    }
}

pub struct InputContext {
    pub plan: InputPlan,
    pub schema: TableSchemaRef,
    pub source: InputSource,
    pub format: Arc<dyn InputFormat>,
    pub splits: Vec<Arc<SplitInfo>>,

    pub format_options: FileFormatOptionsExt,
    // runtime config
    pub settings: Arc<Settings>,

    pub read_batch_size: usize,
    pub block_compact_thresholds: BlockThresholds,

    pub scan_progress: Arc<Progress>,
    pub on_error_mode: OnErrorMode,
    pub on_error_count: AtomicU64,
    pub on_error_map: Option<DashMap<String, HashMap<u16, InputError>>>,
}

impl Debug for InputContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputContext")
            .field("plan", &self.plan)
            .field("block_compact_thresholds", &self.block_compact_thresholds)
            .field("read_batch_size", &self.read_batch_size)
            .field("num_splits", &self.splits.len())
            .finish()
    }
}

impl InputContext {
    pub fn get_input_format(format: &StageFileFormatType) -> Result<Arc<dyn InputFormat>> {
        match format {
            StageFileFormatType::Tsv => Ok(Arc::new(InputFormatTSV::create())),
            StageFileFormatType::Csv => Ok(Arc::new(InputFormatCSV::create())),
            StageFileFormatType::NdJson => Ok(Arc::new(InputFormatNDJson::create())),
            StageFileFormatType::Parquet => Ok(Arc::new(InputFormatParquet {})),
            StageFileFormatType::Xml => Ok(Arc::new(InputFormatXML::create())),
            format => Err(ErrorCode::Internal(format!(
                "Unsupported file format: {:?}",
                format
            ))),
        }
    }

    pub fn try_create_from_copy(
        operator: Operator,
        settings: Arc<Settings>,
        schema: TableSchemaRef,
        stage_info: StageInfo,
        splits: Vec<Arc<SplitInfo>>,
        scan_progress: Arc<Progress>,
        block_compact_thresholds: BlockThresholds,
    ) -> Result<Self> {
        let on_error_mode = stage_info.copy_options.on_error.clone();
        let plan = Box::new(CopyIntoPlan { stage_info });
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let file_format_options = &plan.stage_info.file_format_options;
        let format_typ = file_format_options.format.clone();
        let mut file_format_options = FileFormatOptionsExt::create_from_file_format_options(
            file_format_options.clone(),
            &settings,
        )?;
        file_format_options.check()?;

        let format = Self::get_input_format(&format_typ)?;

        Ok(InputContext {
            format,
            schema,
            splits,
            settings,
            read_batch_size,
            scan_progress,
            source: InputSource::Operator(operator),
            plan: InputPlan::CopyInto(plan),
            block_compact_thresholds,
            format_options: file_format_options,
            on_error_mode,
            on_error_count: AtomicU64::new(0),
            on_error_map: Some(DashMap::new()),
        })
    }

    pub async fn try_create_from_insert_clickhouse(
        format_name: &str,
        stream_receiver: Receiver<Result<StreamingReadBatch>>,
        settings: Arc<Settings>,
        schema: TableSchemaRef,
        scan_progress: Arc<Progress>,
        block_compact_thresholds: BlockThresholds,
    ) -> Result<Self> {
        let (format_name, rows_to_skip) = remove_clickhouse_format_suffix(format_name);

        let typ = ClickhouseFormatType::parse_clickhouse_format(format_name)?;
        let mut file_format_options =
            FileFormatOptionsExt::create_from_clickhouse_format(typ, &settings)?;
        file_format_options.stage.skip_header = rows_to_skip as u64;

        let format_type = file_format_options.stage.format.clone();
        file_format_options.check()?;
        let format = Self::get_input_format(&format_type)?;
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let compression = StageFileCompression::Auto;
        let plan = StreamPlan {
            is_multi_part: false,
            compression,
        };

        Ok(InputContext {
            format,
            schema,
            settings,
            read_batch_size,
            scan_progress,
            source: InputSource::Stream(Mutex::new(Some(stream_receiver))),
            plan: InputPlan::StreamingLoad(plan),
            splits: vec![],
            block_compact_thresholds,
            format_options: file_format_options,
            on_error_mode: OnErrorMode::AbortNum(1),
            on_error_count: AtomicU64::new(0),
            on_error_map: None,
        })
    }

    pub async fn try_create_from_insert_file_format(
        stream_receiver: Receiver<Result<StreamingReadBatch>>,
        settings: Arc<Settings>,
        file_format_options: FileFormatOptions,
        schema: TableSchemaRef,
        scan_progress: Arc<Progress>,
        is_multi_part: bool,
        block_compact_thresholds: BlockThresholds,
    ) -> Result<Self> {
        let read_batch_size = settings.get_input_read_buffer_size()? as usize;
        let format_typ = file_format_options.format.clone();
        let mut file_format_options =
            FileFormatOptionsExt::create_from_file_format_options(file_format_options, &settings)?;
        file_format_options.check()?;
        let format = Self::get_input_format(&format_typ)?;
        let compression = file_format_options.stage.compression;

        let plan = StreamPlan {
            is_multi_part,
            compression,
        };

        Ok(InputContext {
            format,
            schema,
            settings,
            read_batch_size,
            scan_progress,
            source: InputSource::Stream(Mutex::new(Some(stream_receiver))),
            plan: InputPlan::StreamingLoad(plan),
            splits: vec![],
            block_compact_thresholds,
            format_options: file_format_options,
            on_error_mode: OnErrorMode::AbortNum(1),
            on_error_count: AtomicU64::new(0),
            on_error_map: None,
        })
    }

    pub fn num_prefetch_splits(&self) -> Result<usize> {
        Ok(self.settings.get_max_threads()? as usize)
    }

    pub fn num_prefetch_per_split(&self) -> usize {
        1
    }

    pub fn data_schema(&self) -> DataSchema {
        (&self.schema.clone()).into()
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
                return Err(ErrorCode::Unimplemented(
                    "compress type lzo is unimplemented",
                ));
            }
            StageFileCompression::Snappy => {
                return Err(ErrorCode::Unimplemented(
                    "compress type snappy is unimplemented",
                ));
            }
            StageFileCompression::None => None,
        };
        Ok(compression_algo)
    }

    pub fn parse_error_row_based(
        &self,
        reason: &str,
        split_info: &Arc<SplitInfo>,
        offset_in_split: usize,
        row_in_split: usize,
        start_row_of_split: Option<usize>,
    ) -> ErrorCode {
        let offset = offset_in_split + split_info.offset;
        let pos = match start_row_of_split {
            None => {
                format!(
                    "row_in_split={row_in_split}, offset={offset}={}+{offset_in_split}",
                    split_info.offset
                )
            }
            Some(row) => {
                format!(
                    "row={}={row}+{row_in_split}, offset={offset}={}+{offset_in_split}",
                    row + row_in_split,
                    split_info.offset
                )
            }
        };
        let msg = format!(
            "{reason}, split {}, {pos}, options={:?}, schema={:?}",
            split_info,
            self.format_options,
            self.schema.fields()
        );
        ErrorCode::BadBytes(msg)
    }

    pub fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        if let Some(ref on_error_map) = self.on_error_map {
            if on_error_map.is_empty() {
                return None;
            }
            let mut m = HashMap::<String, ErrorCode>::new();
            on_error_map.iter().for_each(|x| {
                if let Some(max_v) = x.value().iter().max_by_key(|entry| entry.1.num) {
                    m.insert(x.key().to_string(), max_v.1.err.clone());
                }
            });
            return Some(m);
        }
        None
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

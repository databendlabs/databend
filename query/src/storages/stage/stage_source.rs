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

use std::collections::VecDeque;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FormatFactory;
use common_formats::InputFormat;
use common_io::prelude::FormatSettings;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::StageTableInfo;
use common_storage::init_operator;
use opendal::Operator;
use parking_lot::Mutex;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Deserializer;
use crate::pipelines::processors::MultiFileSplitter;
use crate::pipelines::processors::OperatorInfo;
use crate::sessions::TableContext;

pub struct StageSourceHelper {
    ctx: Arc<dyn TableContext>,
    operator_info: OperatorInfo,
    file_format: Arc<dyn InputFormat>,
    files: Arc<Mutex<VecDeque<String>>>,
    table_info: StageTableInfo,
}

impl StageSourceHelper {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        schema: DataSchemaRef,
        table_info: StageTableInfo,
        files: Arc<Mutex<VecDeque<String>>>,
    ) -> Result<StageSourceHelper> {
        let stage_info = &table_info.stage_info;
        let file_format_options = &stage_info.file_format_options;
        let mut settings = ctx.get_format_settings()?;
        let size_limit = stage_info.copy_options.size_limit;
        settings.size_limit = if size_limit > 0 {
            Some(size_limit)
        } else {
            None
        };
        settings.skip_header = file_format_options.skip_header > 0;
        settings.field_delimiter = stage_info
            .file_format_options
            .field_delimiter
            .as_bytes()
            .to_vec();
        settings.record_delimiter = stage_info
            .file_format_options
            .record_delimiter
            .as_bytes()
            .to_vec();

        let file_format = Self::get_input_format(&file_format_options.format, schema, settings)?;

        let operator_info = if stage_info.stage_type == StageType::Internal {
            OperatorInfo::Op(ctx.get_storage_operator()?)
        } else {
            OperatorInfo::Cfg(stage_info.stage_params.storage.clone())
        };
        let src = StageSourceHelper {
            ctx,
            operator_info,
            file_format,
            files,
            table_info,
        };
        Ok(src)
    }

    pub fn get_splitter(&self, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        MultiFileSplitter::create(
            self.operator_info.clone(),
            self.ctx.get_scan_progress(),
            output,
            self.file_format.clone(),
            self.table_info.stage_info.file_format_options.compression,
            self.files.clone(),
        )
    }
    pub fn get_deserializer(
        &self,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(Deserializer::create(
            input_port,
            output_port,
            self.file_format.clone(),
        ))
    }

    pub async fn get_op(ctx: &Arc<dyn TableContext>, stage: &UserStageInfo) -> Result<Operator> {
        if stage.stage_type == StageType::Internal {
            ctx.get_storage_operator()
        } else {
            Ok(init_operator(&stage.stage_params.storage).await?)
        }
    }

    fn get_input_format(
        format: &StageFileFormatType,
        schema: DataSchemaRef,
        format_settings: FormatSettings,
    ) -> Result<Arc<dyn InputFormat>> {
        let name = match format {
            StageFileFormatType::Csv => "csv",
            StageFileFormatType::Tsv => "tsv",
            StageFileFormatType::NdJson => "ndjson",
            StageFileFormatType::Parquet => "parquet",
            format => {
                return Err(ErrorCode::LogicalError(format!(
                    "Unsupported file format: {:?}",
                    format
                )));
            }
        };
        let input_format = FormatFactory::instance().get_input(name, schema, format_settings)?;
        Ok(input_format)
    }
}

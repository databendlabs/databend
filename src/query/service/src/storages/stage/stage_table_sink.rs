// Copyright 2021 Datafuse Labs.
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
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::output_format::OutputFormat;
use common_formats::output_format::OutputFormatType;
use common_legacy_planners::StageTableInfo;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use opendal::Operator;

use crate::sessions::TableContext;

const MAX_SIZE_PER_FILE: usize = 256 * 1024 * 1024;

enum State {
    None,
    NeedSerialize(DataBlock),
    NeedWrite(Vec<u8>, Option<DataBlock>),
    Finished,
}

pub struct StageTableSink {
    state: State,
    input: Arc<InputPort>,
    ctx: Arc<dyn TableContext>,
    data_accessor: Operator,
    output: Option<Arc<OutputPort>>,

    table_info: StageTableInfo,
    single: bool,
    working_buffer: Vec<u8>,
    working_datablocks: Vec<DataBlock>,
    output_format: Box<dyn OutputFormat>,
    write_header: bool,

    uuid: String,
    group_id: usize,
    batch_id: usize,
}

impl StageTableSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        table_info: StageTableInfo,
        single: bool,
        data_accessor: Operator,
        output: Option<Arc<OutputPort>>,

        uuid: String,
        group_id: usize,
    ) -> Result<ProcessorPtr> {
        let format_name = format!("{:?}", table_info.stage_info.file_format_options.format);

        let fmt = OutputFormatType::from_str(format_name.as_str())?;
        let mut format_settings = ctx.get_format_settings()?;

        let format_options = &table_info.stage_info.file_format_options;
        {
            if !format_options.field_delimiter.is_empty() {
                format_settings.field_delimiter =
                    format_options.field_delimiter.as_bytes().to_vec();
            }
            if !format_options.record_delimiter.is_empty() {
                format_settings.record_delimiter =
                    format_options.record_delimiter.as_bytes().to_vec();
            }
        }

        let output_format = fmt.create_format(table_info.schema(), format_settings);

        Ok(ProcessorPtr::create(Box::new(StageTableSink {
            ctx,
            input,
            data_accessor,
            table_info,
            state: State::None,
            output,
            single,
            output_format,
            working_buffer: Vec::with_capacity((MAX_SIZE_PER_FILE as f64 * 1.2) as usize),
            working_datablocks: vec![],
            write_header: false,

            uuid,
            group_id,
            batch_id: 0,
        })))
    }

    pub fn unload_path(&self) -> String {
        let format_name = format!(
            "{:?}",
            self.table_info.stage_info.file_format_options.format
        );
        if self.table_info.path.ends_with("data_") {
            format!(
                "{}{}_{}_{}.{}",
                self.table_info.path,
                self.uuid,
                self.group_id,
                self.batch_id,
                format_name.to_ascii_lowercase()
            )
        } else {
            format!(
                "{}/data_{}_{}_{}.{}",
                self.table_info.path,
                self.uuid,
                self.group_id,
                self.batch_id,
                format_name.to_ascii_lowercase()
            )
        }
    }
}

#[async_trait]
impl Processor for StageTableSink {
    fn name(&self) -> &'static str {
        "StageSink"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(&self.state, State::NeedSerialize(_)) {
            return Ok(Event::Sync);
        }

        if matches!(&self.state, State::NeedWrite(_, _)) {
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            let data = std::mem::take(&mut self.working_buffer);
            if data.is_empty() {
                self.state = State::Finished;
                return Ok(Event::Finished);
            }
            match &self.output {
                Some(output) => {
                    for block in &self.working_datablocks {
                        output.push_data(Ok(block.clone()));
                    }
                    self.state = State::Finished;
                    return Ok(Event::Finished);
                }
                None => {
                    self.state = State::NeedWrite(data, None);
                    return Ok(Event::Async);
                }
            }
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.state = State::NeedSerialize(self.input.pull_data().unwrap()?);
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedSerialize(datablock) => {
                let old_pos = self.working_buffer.len();

                if !self.write_header {
                    let prefix = self.output_format.serialize_prefix()?;
                    self.working_buffer.extend_from_slice(&prefix);
                    self.write_header = true;
                }

                let bs = self.output_format.serialize_block(&datablock)?;
                self.working_buffer.extend_from_slice(bs.as_slice());

                if !self.single && self.working_buffer.len() > MAX_SIZE_PER_FILE {
                    let mut data = std::mem::take(&mut self.working_buffer);
                    if old_pos != 0 {
                        data.truncate(old_pos);
                        let final_bytes = self.output_format.finalize()?;
                        data.extend_from_slice(&final_bytes);
                        self.state = State::NeedWrite(data, Some(datablock));
                        return Ok(());
                    } else {
                        let final_bytes = self.output_format.finalize()?;
                        data.extend_from_slice(&final_bytes);
                        self.state = State::NeedWrite(data, None);
                    }
                }

                if self.output.is_some() {
                    self.working_datablocks.push(datablock);
                }
            }
            _state => {
                return Err(ErrorCode::LogicalError(
                    "Unknown state for stage table sink.",
                ));
            }
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedWrite(bytes, remainng_block) => {
                let path = self.unload_path();
                self.ctx
                    .get_dal_context()
                    .get_metrics()
                    .inc_write_bytes(bytes.len());

                let object = self.data_accessor.object(&path);
                object.write(bytes.as_slice()).await?;

                match remainng_block {
                    Some(block) => self.state = State::NeedSerialize(block),
                    None => self.state = State::None,
                }
                self.batch_id += 1;
                Ok(())
            }
            _state => {
                return Err(ErrorCode::LogicalError(
                    "Unknown state for stage table sink.",
                ));
            }
        }
    }
}

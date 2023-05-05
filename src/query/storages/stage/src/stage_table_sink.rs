// Copyright 2021 Datafuse Labs
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
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_formats::output_format::OutputFormat;
use common_formats::FileFormatOptionsExt;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use opendal::Operator;

#[derive(Debug)]
enum State {
    None,
    NeedSerialize(DataBlock),
    NeedWrite(Vec<u8>, Option<DataBlock>),
    Finished,
}

pub struct StageTableSink {
    state: State,
    input: Arc<InputPort>,
    data_accessor: Operator,
    output: Option<Arc<OutputPort>>,

    table_info: StageTableInfo,
    working_buffer: Vec<u8>,
    working_datablocks: Vec<DataBlock>,
    output_format: Box<dyn OutputFormat>,
    write_header: bool,

    uuid: String,
    group_id: usize,
    batch_id: usize,

    single: bool,
    max_file_size: usize,
}

impl StageTableSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        table_info: StageTableInfo,
        data_accessor: Operator,
        output: Option<Arc<OutputPort>>,

        uuid: String,
        group_id: usize,
    ) -> Result<ProcessorPtr> {
        let mut options_ext = FileFormatOptionsExt::create_from_settings(&ctx.get_settings())?;
        let output_format = options_ext.get_output_format(
            table_info.schema(),
            table_info.stage_info.file_format_params.clone(),
        )?;

        let max_file_size = Self::adjust_max_file_size(&ctx, &table_info)?;
        let single = table_info.stage_info.copy_options.single;

        Ok(ProcessorPtr::create(Box::new(StageTableSink {
            input,
            data_accessor,
            table_info,
            state: State::None,
            output,
            single,
            output_format,
            working_buffer: Vec::with_capacity((max_file_size as f64 * 1.2) as usize),
            working_datablocks: vec![],
            write_header: false,

            uuid,
            group_id,
            batch_id: 0,
            max_file_size,
        })))
    }

    fn adjust_max_file_size(
        ctx: &Arc<dyn TableContext>,
        stage_info: &StageTableInfo,
    ) -> Result<usize> {
        // 256M per file by default.
        const DEFAULT_SIZE: usize = 256 * 1024 * 1024;
        // max is half of the max memory usage.
        let max_size = (ctx.get_settings().get_max_memory_usage()? / 2) as usize;

        let mut max_file_size = stage_info.stage_info.copy_options.max_file_size;
        if max_file_size == 0 {
            max_file_size = DEFAULT_SIZE;
        } else if max_file_size > max_size {
            max_file_size = max_size
        }

        Ok(max_file_size)
    }

    pub fn unload_path(&self) -> String {
        let format_name = format!(
            "{:?}",
            self.table_info.stage_info.file_format_params.get_type()
        );

        // assert_eq!("00000110", format!("{:0>8}", "110"))
        if self.table_info.files_info.path.ends_with("data_") {
            format!(
                "{}{}_{:0>4}_{:0>8}.{}",
                self.table_info.files_info.path,
                self.uuid,
                self.group_id,
                self.batch_id,
                format_name.to_ascii_lowercase()
            )
        } else {
            format!(
                "{}/data_{}_{:0>4}_{:0>8}.{}",
                self.table_info.files_info.path,
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
    fn name(&self) -> String {
        "StageSink".to_string()
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
            if self.output_format.buffer_size() > 0 {
                let bs = self.output_format.finalize()?;
                self.working_buffer.extend_from_slice(&bs);
            }
            let data = std::mem::take(&mut self.working_buffer);
            if data.len() >= self.max_file_size || (!data.is_empty() && self.output.is_none()) {
                self.state = State::NeedWrite(data, None);
                self.working_datablocks.clear();
                return Ok(Event::Async);
            }

            match (&self.output, self.working_datablocks.is_empty()) {
                (Some(output), false) => {
                    if output.can_push() {
                        let block = self.working_datablocks.pop().unwrap();
                        output.push_data(Ok(block));
                    }
                    return Ok(Event::NeedConsume);
                }
                _ => {
                    self.state = State::Finished;
                    if let Some(output) = self.output.as_mut() {
                        output.finish()
                    }
                    return Ok(Event::Finished);
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
                if !self.write_header {
                    let prefix = self.output_format.serialize_prefix()?;
                    self.working_buffer.extend_from_slice(&prefix);
                    self.write_header = true;
                }

                if !self.single {
                    for i in (0..datablock.num_rows()).step_by(1024) {
                        let end = (i + 1024).min(datablock.num_rows());
                        let small_block = datablock.slice(i..end);

                        let bs = self.output_format.serialize_block(&small_block)?;
                        self.working_buffer.extend_from_slice(bs.as_slice());

                        if self.working_buffer.len() + self.output_format.buffer_size()
                            >= self.max_file_size
                        {
                            let bs = self.output_format.finalize()?;
                            self.working_buffer.extend_from_slice(&bs);

                            let data = std::mem::take(&mut self.working_buffer);
                            self.working_datablocks.clear();
                            if end != datablock.num_rows() {
                                let remain = datablock.slice(end..datablock.num_rows());
                                self.state = State::NeedWrite(data, Some(remain));
                            } else {
                                self.state = State::NeedWrite(data, None);
                            }
                            return Ok(());
                        }
                    }
                } else {
                    let bs = self.output_format.serialize_block(&datablock)?;
                    self.working_buffer.extend_from_slice(bs.as_slice());
                }

                // hold this datablock
                if self.output.is_some() {
                    self.working_datablocks.push(datablock);
                }
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for stage table sink."));
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedWrite(bytes, remaining_block) => {
                let path = self.unload_path();

                self.data_accessor.write(&path, bytes).await?;

                match remaining_block {
                    Some(block) => self.state = State::NeedSerialize(block),
                    None => self.state = State::None,
                }
                self.batch_id += 1;
                Ok(())
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for stage table sink."));
            }
        }
    }
}

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
use std::io::Cursor;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BufferReadExt;
use common_io::prelude::BufferReader;
use common_io::prelude::CheckpointReader;
use common_io::prelude::FormatSettings;

use crate::formats::FormatFactory;
use crate::formats::InputFormat;
use crate::formats::InputState;

pub struct CsvInputState {
    pub quotes: u8,
    pub memory: Vec<u8>,
    pub accepted_rows: usize,
    pub accepted_bytes: usize,
    pub need_more_data: bool,
    pub ignore_if_first: Option<u8>,
}

impl InputState for CsvInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct CsvInputFormat {
    schema: DataSchemaRef,
    field_delimiter: u8,
    need_skip_header: bool,
    row_delimiter: Option<u8>,
    min_accepted_rows: usize,
    min_accepted_bytes: usize,
    settings: FormatSettings,
}

impl CsvInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        factory.register_input(
            "csv",
            Box::new(
                |name: &str, schema: DataSchemaRef, settings: FormatSettings| {
                    CsvInputFormat::try_create(name, schema, settings, 8192, 10 * 1024 * 1024)
                },
            ),
        )
    }

    pub fn try_create(
        _name: &str,
        schema: DataSchemaRef,
        settings: FormatSettings,
        min_accepted_rows: usize,
        min_accepted_bytes: usize,
    ) -> Result<Box<dyn InputFormat>> {
        let field_delimiter = match settings.field_delimiter.len() {
            n if n >= 1 => settings.field_delimiter[0],
            _ => b',',
        };

        let mut row_delimiter = None;

        if !settings.record_delimiter.is_empty()
            && settings.record_delimiter[0] != b'\n'
            && settings.record_delimiter[0] != b'\r'
        {
            row_delimiter = Some(settings.record_delimiter[0]);
        }

        let need_skip_header = settings.skip_header;

        Ok(Box::new(CsvInputFormat {
            schema,
            settings,
            row_delimiter,
            field_delimiter,
            need_skip_header,
            min_accepted_rows,
            min_accepted_bytes,
        }))
    }

    fn find_quotes(buf: &[u8], pos: usize, state: &mut CsvInputState) -> usize {
        for (index, byte) in buf.iter().enumerate().skip(pos) {
            if *byte == b'"' || *byte == b'\'' {
                state.quotes = 0;
                return index + 1;
            }
        }

        buf.len()
    }

    fn find_delimiter(&self, buf: &[u8], pos: usize, state: &mut CsvInputState) -> usize {
        for index in pos..buf.len() {
            if buf[index] == b'"' || buf[index] == b'\'' {
                state.quotes = buf[index];
                return index + 1;
            }

            if let Some(b) = &self.row_delimiter {
                if buf[index] == *b {
                    return self.accept_row::<0>(buf, pos, state, index);
                }
            } else if buf[index] == b'\r' {
                return self.accept_row::<b'\n'>(buf, pos, state, index);
            } else if buf[index] == b'\n' {
                return self.accept_row::<b'\r'>(buf, pos, state, index);
            }
        }

        buf.len()
    }

    #[inline(always)]
    fn accept_row<const C: u8>(
        &self,
        buf: &[u8],
        pos: usize,
        state: &mut CsvInputState,
        index: usize,
    ) -> usize {
        state.accepted_rows += 1;
        state.accepted_bytes += index - pos;

        if state.accepted_rows >= self.min_accepted_rows
            || (state.accepted_bytes + index) >= self.min_accepted_bytes
        {
            state.need_more_data = false;
        }

        if C != 0 {
            if buf.len() <= index + 1 {
                state.ignore_if_first = Some(C);
            } else if buf[index + 1] == C {
                return index + 2;
            }
        }

        index + 1
    }
}

impl InputFormat for CsvInputFormat {
    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(CsvInputState {
            quotes: 0,
            memory: vec![],
            accepted_rows: 0,
            accepted_bytes: 0,
            need_more_data: false,
            ignore_if_first: None,
        })
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<DataBlock> {
        let mut deserializers = Vec::with_capacity(self.schema.num_fields());
        for field in self.schema.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(self.min_accepted_rows));
        }

        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();
        let cursor = Cursor::new(&state.memory);
        let reader = BufferReader::new(cursor);
        let mut checkpoint_reader = CheckpointReader::new(reader);

        for row_index in 0..self.min_accepted_rows {
            if checkpoint_reader.eof()? {
                break;
            }

            for column_index in 0..deserializers.len() {
                if checkpoint_reader.ignore_white_spaces_and_byte(self.field_delimiter)? {
                    deserializers[column_index].de_default(&self.settings);
                } else {
                    deserializers[column_index]
                        .de_text_csv(&mut checkpoint_reader, &self.settings)?;

                    if column_index + 1 != deserializers.len() {
                        checkpoint_reader
                            .must_ignore_white_spaces_and_byte(self.field_delimiter)?;
                    }
                }
            }

            checkpoint_reader.ignore_white_spaces_and_byte(self.field_delimiter)?;

            if let Some(delimiter) = &self.row_delimiter {
                if !checkpoint_reader.ignore_white_spaces_and_byte(*delimiter)?
                    && !checkpoint_reader.eof()?
                {
                    return Err(ErrorCode::BadBytes(format!(
                        "Parse csv error at line {}",
                        row_index
                    )));
                }
            } else {
                if (!checkpoint_reader.ignore_white_spaces_and_byte(b'\n')?
                    & !checkpoint_reader.ignore_white_spaces_and_byte(b'\r')?)
                    && !checkpoint_reader.eof()?
                {
                    return Err(ErrorCode::BadBytes(format!(
                        "Parse csv error at line {}",
                        row_index
                    )));
                }

                // \r\n
                checkpoint_reader.ignore_white_spaces_and_byte(b'\n')?;
            }
        }

        let mut columns = Vec::with_capacity(deserializers.len());
        for deserializer in &mut deserializers {
            columns.push(deserializer.finish_to_column());
        }

        Ok(DataBlock::create(self.schema.clone(), columns))
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize> {
        let mut index = 0;
        let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();

        if let Some(first) = state.ignore_if_first.take() {
            if buf[0] == first {
                index += 1;
            }
        }

        state.need_more_data = true;
        while index < buf.len() && state.need_more_data {
            index = match state.quotes != 0 {
                true => Self::find_quotes(buf, index, state),
                false => self.find_delimiter(buf, index, state),
            }
        }

        state.memory.extend_from_slice(&buf[0..index]);
        Ok(index)
    }

    fn skip_header(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize> {
        if self.need_skip_header {
            let mut index = 0;
            let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();

            while index < buf.len() {
                index = match state.quotes != 0 {
                    true => Self::find_quotes(buf, index, state),
                    false => self.find_delimiter(buf, index, state),
                };

                if state.accepted_rows == 1 {
                    return Ok(index);
                }
            }
        }

        Ok(0)
    }
}

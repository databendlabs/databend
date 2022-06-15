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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::position2;
use common_io::prelude::position4;
use common_io::prelude::BufferReadExt;
use common_io::prelude::FormatSettings;
use common_io::prelude::MemoryReader;

use crate::formats::FormatFactory;
use crate::formats::InputFormat;
use crate::formats::InputState;

pub struct TsvInputState {
    pub quotes: u8,
    pub memory: Vec<u8>,
    pub accepted_rows: usize,
    pub accepted_bytes: usize,
    pub need_more_data: bool,
    pub ignore_if_first: Option<u8>,
}

impl InputState for TsvInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct TsvInputFormat {
    schema: DataSchemaRef,
    skip_rows: usize,
    min_accepted_rows: usize,
    min_accepted_bytes: usize,
    settings: FormatSettings,
}

impl TsvInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        macro_rules! register {
            ($name: expr, $skip_rows: expr) => {
                factory.register_input(
                    $name,
                    Box::new(
                        |name: &str, schema: DataSchemaRef, settings: FormatSettings| {
                            TsvInputFormat::try_create(
                                name,
                                schema,
                                settings,
                                $skip_rows,
                                8192,
                                10 * 1024 * 1024,
                            )
                        },
                    ),
                );
            };
        }

        register! { "Tsv", 0 }
        register! { "TsvWithNames", 1 }
        register! { "TsvWithNamesAndTypes", 2 }

        register! { "TabSeparated", 0 }
        register! { "TabSeparatedWithNames", 1 }
        register! { "TabSeparatedWithNamesAndTypes", 2 }
    }

    pub fn try_create(
        _name: &str,
        schema: DataSchemaRef,
        settings: FormatSettings,
        skip_rows: usize,
        min_accepted_rows: usize,
        min_accepted_bytes: usize,
    ) -> Result<Box<dyn InputFormat>> {
        Ok(Box::new(TsvInputFormat {
            schema,
            settings,
            skip_rows,
            min_accepted_rows,
            min_accepted_bytes,
        }))
    }

    fn find_quotes(buf: &[u8], pos: usize, state: &mut TsvInputState) -> usize {
        let index = pos + position2::<true, b'"', b'\''>(&buf[pos..]);

        if index != buf.len() {
            state.quotes = 0;
            return index + 1;
        }

        buf.len()
    }

    fn find_delimiter(&self, buf: &[u8], pos: usize, state: &mut TsvInputState) -> usize {
        let position = pos + position4::<true, b'"', b'\'', b'\r', b'\n'>(&buf[pos..]);

        if position != buf.len() {
            if buf[position] == b'"' || buf[position] == b'\'' {
                state.quotes = buf[position];
                return position + 1;
            } else if buf[position] == b'\r' {
                return self.accept_row::<b'\n'>(buf, pos, state, position);
            } else if buf[position] == b'\n' {
                return self.accept_row::<b'\r'>(buf, pos, state, position);
            }
        }
        buf.len()
    }

    #[inline(always)]
    fn accept_row<const C: u8>(
        &self,
        buf: &[u8],
        pos: usize,
        state: &mut TsvInputState,
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

impl InputFormat for TsvInputFormat {
    fn support_parallel(&self) -> bool {
        true
    }

    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(TsvInputState {
            quotes: 0,
            memory: vec![],
            accepted_rows: 0,
            accepted_bytes: 0,
            need_more_data: false,
            ignore_if_first: None,
        })
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>> {
        let mut deserializers = Vec::with_capacity(self.schema.num_fields());
        for field in self.schema.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(self.min_accepted_rows));
        }

        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();
        let memory = std::mem::take(&mut state.memory);
        let mut memory_reader = MemoryReader::new(memory);

        let mut row_index = 0;
        while !memory_reader.eof()? {
            for column_index in 0..deserializers.len() {
                if memory_reader.ignore_white_spaces_and_byte(b'\t')? {
                    deserializers[column_index].de_default(&self.settings);
                } else {
                    deserializers[column_index].de_text_csv(&mut memory_reader, &self.settings)?;

                    if column_index + 1 != deserializers.len() {
                        memory_reader.must_ignore_white_spaces_and_byte(b'\t')?;
                    }
                }
            }

            memory_reader.ignore_white_spaces_and_byte(b'\t')?;

            if (!memory_reader.ignore_white_spaces_and_byte(b'\n')?
                & !memory_reader.ignore_white_spaces_and_byte(b'\r')?)
                && !memory_reader.eof()?
            {
                return Err(ErrorCode::BadBytes(format!(
                    "Parse Tsv error at line {}",
                    row_index
                )));
            }

            // \r\n
            memory_reader.ignore_white_spaces_and_byte(b'\n')?;

            row_index += 1;
        }

        let mut columns = Vec::with_capacity(deserializers.len());
        for deserializer in &mut deserializers {
            columns.push(deserializer.finish_to_column());
        }

        Ok(vec![DataBlock::create(self.schema.clone(), columns)])
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize> {
        let mut index = 0;
        let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();

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
        if self.skip_rows > 0 {
            let mut index = 0;
            let state = state.as_any().downcast_mut::<TsvInputState>().unwrap();

            while index < buf.len() {
                index = match state.quotes != 0 {
                    true => Self::find_quotes(buf, index, state),
                    false => self.find_delimiter(buf, index, state),
                };

                if state.accepted_rows == self.skip_rows {
                    return Ok(index);
                }
            }
        }
        Ok(0)
    }
}

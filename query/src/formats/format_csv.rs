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
use common_io::prelude::BufferRead;
use common_io::prelude::BufferReadExt;
use common_io::prelude::BufferReader;
use common_io::prelude::CheckpointReader;
use common_io::prelude::FormatSettings;

use crate::formats::FormatFactory;
use crate::formats::InputFormat;
use crate::formats::InputState;

pub struct CsvInputState {
    pub quotes: bool,
    pub memory: Vec<u8>,
    pub accepted_rows: usize,
    pub accepted_bytes: usize,
    pub ignore_if_first_is_r: bool,
    pub ignore_if_first_is_n: bool,
}

impl InputState for CsvInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct CsvInputFormat {
    schema: DataSchemaRef,
    min_accepted_rows: usize,
    min_accepted_bytes: usize,
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
        _settings: FormatSettings,
        min_accepted_rows: usize,
        min_accepted_bytes: usize,
    ) -> Result<Box<dyn InputFormat>> {
        // let field_delimiter = match settings.field_delimiter.len() {
        //     n if n >= 1 => settings.field_delimiter[0],
        //     _ => b',',
        // };
        //
        // let record_delimiter = match settings.record_delimiter.len() {
        //     n if n >= 1 => settings.record_delimiter[0],
        //     _ => b'\n',
        // };

        // let record_delimiter = if record_delimiter == b'\n' || record_delimiter == b'\r' {
        //     Terminator::CRLF
        // } else {
        //     Terminator::Any(record_delimiter)
        // };

        // let skip_header = settings.skip_header;
        // let empty_as_default = settings.empty_as_default;

        Ok(Box::new(CsvInputFormat {
            schema,
            min_accepted_rows,
            min_accepted_bytes,
        }))
    }

    fn find_quotes(buf: &[u8], pos: usize, state: &mut CsvInputState) -> usize {
        for (index, byte) in buf.iter().enumerate().skip(pos) {
            if *byte == b'"' {
                state.quotes = false;
                return index + 1;
            }
        }

        buf.len()
    }

    fn find_delimiter(
        &self,
        buf: &[u8],
        pos: usize,
        state: &mut CsvInputState,
        more_data: &mut bool,
    ) -> usize {
        for index in pos..buf.len() {
            match buf[index] {
                b'"' => {
                    state.quotes = true;
                    return index + 1;
                }
                b'\r' => {
                    state.accepted_rows += 1;
                    if state.accepted_rows >= self.min_accepted_rows
                        || (state.accepted_bytes + index) >= self.min_accepted_bytes
                    {
                        *more_data = false;
                    }

                    if buf.len() <= index + 1 {
                        state.ignore_if_first_is_n = true;
                    } else if buf[index + 1] == b'\n' {
                        return index + 2;
                    }

                    return index + 1;
                }
                b'\n' => {
                    state.accepted_rows += 1;
                    if state.accepted_rows >= self.min_accepted_rows
                        || (state.accepted_bytes + index) >= self.min_accepted_bytes
                    {
                        *more_data = false;
                    }

                    if buf.len() <= index + 1 {
                        state.ignore_if_first_is_r = true;
                    } else if buf[index + 1] == b'\r' {
                        return index + 2;
                    }

                    return index + 1;
                }
                _ => { /*do nothing*/ }
            }
        }

        buf.len()
    }
}

impl InputFormat for CsvInputFormat {
    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(CsvInputState {
            quotes: false,
            memory: vec![],
            accepted_rows: 0,
            accepted_bytes: 0,
            ignore_if_first_is_r: false,
            ignore_if_first_is_n: false,
        })
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<DataBlock> {
        let mut deserializers = Vec::with_capacity(self.schema.num_fields());
        for field in self.schema.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(self.min_accepted_rows));
        }

        let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();
        let cursor = Cursor::new(&state.memory);
        let reader: Box<dyn BufferRead> = Box::new(BufferReader::new(cursor));
        let mut checkpoint_reader = CheckpointReader::new(reader);

        for row_index in 0..self.min_accepted_rows {
            if checkpoint_reader.eof()? {
                break;
            }

            for column_index in 0..deserializers.len() {
                if checkpoint_reader.ignore_byte(b'\t')? {
                    deserializers[column_index].de_default();
                } else {
                    deserializers[column_index].de_text_csv(&mut checkpoint_reader)?;

                    if column_index + 1 != deserializers.len() {
                        checkpoint_reader.must_ignore_byte(b'\t')?;
                    }
                }
            }

            if !checkpoint_reader.ignore_byte(b'\n')? & !checkpoint_reader.ignore_byte(b'\r')? {
                return Err(ErrorCode::BadBytes(format!(
                    "Parse csv error at line {}",
                    row_index
                )));
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
        let mut need_more_data = true;
        let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();

        if state.ignore_if_first_is_r {
            if buf[0] == b'\r' {
                index += 1;
            }
        } else if state.ignore_if_first_is_n && buf[0] == b'\n' {
            index += 1;
        }

        while index < buf.len() && need_more_data {
            index = match state.quotes {
                true => Self::find_quotes(buf, index, state),
                false => self.find_delimiter(buf, index, state, &mut need_more_data),
            }
        }

        state.memory.extend_from_slice(&buf[0..index]);
        Ok(index)
    }
}

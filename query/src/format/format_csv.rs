use std::any::Any;
use std::io::Cursor;
use std::ops::Deref;
use common_arrow::arrow::io::csv;
use common_arrow::arrow::io::csv::read::ByteRecord;
use common_datablocks::DataBlock;
use common_datavalues::{DataSchemaRef, TypeDeserializerImpl};
use crate::format::{FormatFactory, InputFormat, InputState};
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_datavalues::DataType;

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
        factory.register_input("csv", Box::new(|name: &str, schema: DataSchemaRef, settings: FormatSettings| {
            CsvInputFormat::try_create(name, schema, settings, 8192, 10 * 1024 * 1024)
        }))
    }

    pub fn try_create(name: &str, schema: DataSchemaRef, settings: FormatSettings, min_accepted_rows: usize, min_accepted_bytes: usize) -> Result<Box<dyn InputFormat>> {
        Ok(Box::new(CsvInputFormat {
            schema,
            min_accepted_rows,
            min_accepted_bytes: min_accepted_bytes,
        }))
    }

    fn find_quotes(buf: &[u8], pos: usize, state: &mut CsvInputState) -> usize {
        for index in pos..buf.len() {
            if buf[index] == b'"' {
                state.quotes = false;
                return index + 1;
            }
        }

        buf.len()
    }

    fn find_delimiter(&self, buf: &[u8], pos: usize, state: &mut CsvInputState, more_data: &mut bool) -> usize {
        for index in pos..buf.len() {
            match buf[index] {
                b'"' => {
                    state.quotes = true;
                    return index + 1;
                }
                b'\r' => {
                    state.accepted_rows += 1;
                    if state.accepted_rows >= self.min_accepted_rows || (state.accepted_bytes + index) >= self.min_accepted_bytes {
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
                    if state.accepted_rows >= self.min_accepted_rows || (state.accepted_bytes + index) >= self.min_accepted_bytes {
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
        for field in self.schema.fields() {
            let data_type = field.data_type();
            let deserializer = data_type.create_deserializer(self.min_accepted_rows);
            // deserializer
        }
        todo!()
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize> {
        let mut index = 0;
        let mut need_more_data = true;
        let state = state.as_any().downcast_mut::<CsvInputState>().unwrap();

        if state.ignore_if_first_is_r {
            if buf[0] == b'\r' {
                index += 1;
            }
        } else if state.ignore_if_first_is_n {
            if buf[0] == b'\n' {
                index += 1;
            }
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

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
use std::borrow::Cow;
use std::io::BufRead;
use std::io::Cursor;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::position2;
use common_io::prelude::FileSplit;
use common_io::prelude::FormatSettings;

use crate::FormatFactory;
use crate::InputFormat;
use crate::InputState;

pub struct NDJsonInputState {
    pub memory: Vec<u8>,
    pub accepted_rows: usize,
    pub accepted_bytes: usize,
    pub need_more_data: bool,
    pub ignore_if_first: Option<u8>,
    pub start_row_index: usize,
    pub file_name: Option<String>,
}

impl InputState for NDJsonInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct NDJsonInputFormat {
    schema: DataSchemaRef,
    min_accepted_rows: usize,
    min_accepted_bytes: usize,

    ident_case_sensitive: bool,
    settings: FormatSettings,
}

impl NDJsonInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        factory.register_input(
            "NDJson",
            Box::new(
                |name: &str, schema: DataSchemaRef, settings: FormatSettings| {
                    NDJsonInputFormat::try_create(name, schema, settings, 8192, 10 * 1024 * 1024)
                },
            ),
        );
    }

    pub fn try_create(
        _name: &str,
        schema: DataSchemaRef,
        settings: FormatSettings,
        min_accepted_rows: usize,
        min_accepted_bytes: usize,
    ) -> Result<Arc<dyn InputFormat>> {
        let ident_case_sensitive = settings.ident_case_sensitive;

        Ok(Arc::new(NDJsonInputFormat {
            schema,
            settings,
            min_accepted_rows,
            min_accepted_bytes,
            ident_case_sensitive,
        }))
    }

    fn find_delimiter(&self, buf: &[u8], pos: usize, state: &mut NDJsonInputState) -> usize {
        let position = pos + position2::<true, b'\r', b'\n'>(&buf[pos..]);

        if position != buf.len() {
            if buf[position] == b'\r' {
                return self.accept_row::<b'\n'>(buf, pos, state, position);
            } else if buf[position] == b'\n' {
                return self.accept_row::<0>(buf, pos, state, position);
            }
        }
        buf.len()
    }

    #[inline(always)]
    fn accept_row<const C: u8>(
        &self,
        buf: &[u8],
        pos: usize,
        state: &mut NDJsonInputState,
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

impl InputFormat for NDJsonInputFormat {
    fn support_parallel(&self) -> bool {
        true
    }

    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(NDJsonInputState {
            memory: vec![],
            accepted_rows: 0,
            accepted_bytes: 0,
            need_more_data: false,
            ignore_if_first: None,
            start_row_index: 0,
            file_name: None,
        })
    }

    fn set_state(
        &self,
        state: &mut Box<dyn InputState>,
        file_name: String,
        start_row_index: usize,
    ) -> Result<()> {
        let state = state.as_any().downcast_mut::<NDJsonInputState>().unwrap();
        state.file_name = Some(file_name);
        state.start_row_index = start_row_index;
        Ok(())
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>> {
        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<NDJsonInputState>().unwrap();
        let memory = std::mem::take(&mut state.memory);
        self.deserialize_complete_split(FileSplit {
            path: state.file_name.clone(),
            start_offset: 0,
            start_row: state.start_row_index,
            buf: memory,
        })
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<(usize, bool)> {
        let mut index = 0;
        let state = state.as_any().downcast_mut::<NDJsonInputState>().unwrap();

        if let Some(first) = state.ignore_if_first.take() {
            if buf[0] == first {
                index += 1;
            }
        }

        state.need_more_data = true;
        while index < buf.len() && state.need_more_data {
            index = self.find_delimiter(buf, index, state);
        }

        state.memory.extend_from_slice(&buf[0..index]);
        let finished = !state.need_more_data && state.ignore_if_first.is_none();
        Ok((index, finished))
    }

    fn take_buf(&self, state: &mut Box<dyn InputState>) -> Vec<u8> {
        let state = state.as_any().downcast_mut::<NDJsonInputState>().unwrap();
        std::mem::take(&mut state.memory)
    }

    fn skip_header(&self, _: &[u8], _: &mut Box<dyn InputState>, _: usize) -> Result<usize> {
        Ok(0)
    }

    fn read_row_num(&self, state: &mut Box<dyn InputState>) -> Result<usize> {
        let state = state.as_any().downcast_mut::<NDJsonInputState>().unwrap();
        Ok(state.accepted_rows)
    }

    fn deserialize_complete_split(&self, split: FileSplit) -> Result<Vec<DataBlock>> {
        let mut deserializers = self.schema.create_deserializers(self.min_accepted_rows);
        let mut reader = Cursor::new(split.buf);

        let mut buf = String::new();
        let mut rows = 0;

        loop {
            rows += 1;

            buf.clear();
            let size = reader.read_line(&mut buf)?;
            if size == 0 {
                break;
            }

            if buf.trim().is_empty() {
                continue;
            }

            let mut json: serde_json::Value = serde_json::from_reader(buf.trim().as_bytes())?;
            // if it's not case_sensitive, we convert to lowercase
            if !self.ident_case_sensitive {
                if let serde_json::Value::Object(x) = json {
                    let y = x.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
                    json = serde_json::Value::Object(y);
                }
            }

            for (f, deser) in self.schema.fields().iter().zip(deserializers.iter_mut()) {
                let value = if self.ident_case_sensitive {
                    &json[f.name().to_owned()]
                } else {
                    &json[f.name().to_lowercase()]
                };

                deser.de_json(value, &self.settings).map_err(|e| {
                    let value_str = format!("{:?}", value);
                    ErrorCode::BadBytes(format!(
                        "error at row {} column {}: err={}, value={}",
                        rows,
                        f.name(),
                        e.message(),
                        maybe_truncated(&value_str, 1024),
                    ))
                })?;
            }
        }
        let mut columns = Vec::with_capacity(deserializers.len());
        for deserializer in &mut deserializers {
            columns.push(deserializer.finish_to_column());
        }

        Ok(vec![DataBlock::create(self.schema.clone(), columns)])
    }
}

fn maybe_truncated(s: &str, limit: usize) -> Cow<'_, str> {
    if s.len() > limit {
        Cow::Owned(format!(
            "(first {}B of {}B): {}",
            limit,
            s.len(),
            &s[..limit]
        ))
    } else {
        Cow::Borrowed(s)
    }
}

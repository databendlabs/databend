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

use std::fmt::Write;

use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializerImpl;
use common_exception::Result;
use common_io::prelude::BufferReadExt;
use common_io::prelude::FormatSettings;
use common_io::prelude::MemoryReader;
use common_io::prelude::NestedCheckpointReader;

pub fn verbose_string(buf: &[u8], out: &mut String) {
    out.push('"');

    if buf.is_empty() {
        out.push_str("<EMPTY>");
    }

    for c in buf.iter() {
        match *c {
            b'\0' => {
                out.push_str("<ASCII NUL>");
            }
            b'\n' => {
                out.push_str("<LINE FEED>");
            }
            b'\r' => out.push_str("<CARRIAGE RETURN>"),
            b'\t' => {
                out.push_str("<TAB>");
            }
            b'\\' => {
                out.push_str("<BACKSLASH>");
            }
            b'"' => {
                out.push_str("<DOUBLE QUOTE>");
            }
            b'\'' => {
                out.push_str("<SINGLE QUOTE>");
            }
            _ => {
                out.push(*c as char);
            }
        }
    }
    out.push('"');
}

pub trait FormatDiagnostic {
    fn get_diagnostic_info(
        &self,
        buf: &[u8],
        row_index: usize,
        schema: DataSchemaRef,
        min_accepted_rows: usize,
        settings: FormatSettings,
    ) -> Result<String> {
        let mut out = String::new();
        write!(out, "\nError occurs at row {}:\n", row_index).unwrap();
        self.parse_row_and_print_diagnostic_info(
            buf,
            schema,
            min_accepted_rows,
            settings,
            &mut out,
        )?;
        Ok(out)
    }

    fn parse_row_and_print_diagnostic_info(
        &self,
        buf: &[u8],
        schema: DataSchemaRef,
        min_accepted_rows: usize,
        settings: FormatSettings,
        out: &mut String,
    ) -> Result<bool> {
        let mut row_data: Vec<u8> = Vec::new();
        row_data.extend_from_slice(buf);
        let memory = std::mem::take(&mut row_data);
        let memory_reader = MemoryReader::new(memory);
        let mut checkpoint_reader = NestedCheckpointReader::new(memory_reader);

        if checkpoint_reader.eof()? {
            out.push_str("\t<End of stream>\n");
            return Ok(false);
        }

        let mut deserializers = Vec::with_capacity(schema.num_fields());
        for field in schema.fields() {
            let data_type = field.data_type();
            deserializers.push(data_type.create_deserializer(min_accepted_rows));
        }

        if !self.parse_row_between_delimiter_with_diagnostic_info(out)? {
            return Ok(false);
        }

        if !self.parse_row_start_with_diagnostic_info(out)? {
            return Ok(false);
        }

        for (column_index, deserializer) in deserializers.iter_mut().enumerate() {
            if !self.deserialize_field_and_print_diagnositc_info(
                column_index,
                deserializer,
                &mut checkpoint_reader,
                settings.clone(),
                out,
            )? {
                return Ok(false);
            }

            if column_index + 1 != schema.fields().len()
                && !self.parse_field_delimiter_with_diagnostic_info(&mut checkpoint_reader, out)?
            {
                return Ok(false);
            }
        }

        self.parse_row_end_with_diagnostic_info(&mut checkpoint_reader, out)
    }

    fn parse_row_between_delimiter_with_diagnostic_info(&self, _out: &mut String) -> Result<bool> {
        Ok(true)
    }

    fn parse_row_start_with_diagnostic_info(&self, _out: &mut String) -> Result<bool> {
        Ok(true)
    }

    fn deserialize_field_and_print_diagnositc_info(
        &self,
        col_index: usize,
        deserializer: &mut TypeDeserializerImpl,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        settings: FormatSettings,
        out: &mut String,
    ) -> Result<bool>;

    fn parse_field_delimiter_with_diagnostic_info(
        &self,
        _checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        _out: &mut String,
    ) -> Result<bool> {
        Ok(true)
    }

    fn parse_row_end_with_diagnostic_info(
        &self,
        _checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        _out: &mut String,
    ) -> Result<bool> {
        Ok(true)
    }
}

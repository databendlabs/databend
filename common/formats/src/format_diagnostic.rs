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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializerImpl;
use common_exception::Result;
use common_io::prelude::BufferReadExt;
use common_io::prelude::FormatSettings;
use common_io::prelude::MemoryReader;
use common_io::prelude::NestedCheckpointReader;

static ASCII_TABLE: [&str; 128] = [
    "<NUL>",
    "<SOH>",
    "<STX>",
    "<ETX>",
    "<EOT>",
    "<ENQ>",
    "<ACK>",
    "<BEL>",
    "<BACKSPACE>",
    "<HORIZONTAL TAB>",
    "<LINE FEED>",
    "<VERTICAL TAB>",
    "<FROM FEED>",
    "<CARRIAGE RETURN>",
    "<SO>",
    "<SI>",
    "<DLE>",
    "<DC1/XON>",
    "<DC2>",
    "<DC3/XOFF>",
    "<DC4>",
    "<NAK>",
    "<SYN>",
    "<ETB>",
    "<CAN>",
    "<EM>",
    "<SUB>",
    "<ESC>",
    "<FS>",
    "<GS>",
    "<RS>",
    "<US>",
    " ",
    "!",
    "<DOUBLE QUOTE>",
    "#",
    "$",
    "%",
    "&",
    "<SINGLE QUOTE>",
    "(",
    ")",
    "*",
    "+",
    ",",
    "-",
    ".",
    "/",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    ":",
    ";",
    "<",
    "=",
    ">",
    "?",
    "@",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "[",
    "<BACKSLASH>",
    "]",
    "^",
    "_",
    "`",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
    "{",
    "|",
    "}",
    "~",
    "<DEL>",
];

pub fn verbose_string(buf: &[u8], out: &mut String) {
    out.push('"');

    if buf.is_empty() {
        out.push_str("<EMPTY>");
    }

    for &c in buf.iter() {
        if c < 128 {
            out.push_str(ASCII_TABLE[c as usize]);
        } else {
            out.push(c as char);
        }
    }

    out.push('"');
}

#[allow(clippy::format_push_string)]
pub trait FormatDiagnostic {
    fn get_diagnostic_info(
        &self,
        buf: &[u8],
        file_name: &Option<String>,
        row_index: usize,
        schema: DataSchemaRef,
        min_accepted_rows: usize,
        settings: FormatSettings,
    ) -> Result<String> {
        let mut out = String::new();
        match file_name {
            Some(file_name) => {
                out.push_str(&format!(
                    "\nError occurs at file: {}, row: {}:\n",
                    file_name, row_index
                ));
            }
            None => {
                out.push_str(&format!("\nError occurs at row: {}:\n", row_index));
            }
        }
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
            if !self.parse_field_start_with_diagnostic_info(&mut checkpoint_reader, out)? {
                return Ok(false);
            }

            if !self.deserialize_field_and_print_diagnositc_info(
                schema.clone(),
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

    fn parse_field_start_with_diagnostic_info(
        &self,
        _checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        _out: &mut String,
    ) -> Result<bool> {
        Ok(true)
    }

    fn deserialize_field_and_print_diagnositc_info(
        &self,
        schema: DataSchemaRef,
        col_index: usize,
        deserializer: &mut TypeDeserializerImpl,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        settings: FormatSettings,
        out: &mut String,
    ) -> Result<bool> {
        let col_name = schema.field(col_index).name();
        let data_type = schema.field(col_index).data_type();

        out.push_str(&format!(
            "\tColumn: {}, Name: {}, Type: {}",
            col_index,
            col_name,
            data_type.data_type_id()
        ));

        checkpoint_reader.push_checkpoint();

        let has_err = self.deserialize_field(deserializer, checkpoint_reader, settings);

        let data_type_id = data_type.data_type_id();
        if (data_type_id.is_integer() || data_type_id.is_date_or_date_time())
            && checkpoint_reader.get_top_checkpoint_pos() == checkpoint_reader.pos
        {
            out.push_str("\tError: text ");
            let mut buf: Vec<u8> = Vec::new();
            checkpoint_reader.positionn(10, &mut buf)?;
            verbose_string(&buf, out);
            out.push_str(&format!(" is not like {}\n", data_type_id));
            checkpoint_reader.pop_checkpoint();
            return Ok(false);
        }

        out.push_str(", Parsed text: ");
        verbose_string(checkpoint_reader.get_checkpoint_buffer(), out);
        out.push('\n');
        checkpoint_reader.pop_checkpoint();

        if has_err.is_err() {
            if data_type.data_type_id().is_date_time() {
                out.push_str("\tERROR: DateTime must be in YYYY-MM-DD hh:mm:ss format.\n");
            } else if data_type.data_type_id().is_date() {
                out.push_str("\tERROR: Date must be in YYYY-MM-DD format.\n");
            } else {
                out.push_str("\tERROR\n")
            }
            return Ok(false);
        }

        Ok(true)
    }

    fn deserialize_field(
        &self,
        deserializer: &mut TypeDeserializerImpl,
        checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        settings: FormatSettings,
    ) -> Result<()>;

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

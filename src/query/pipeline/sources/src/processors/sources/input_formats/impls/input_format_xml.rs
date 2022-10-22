//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BufferReadExt;
use common_io::prelude::FormatSettings;
use common_io::prelude::NestedCheckpointReader;
use common_meta_types::StageFileFormatType;
use common_settings::Settings;
use xml::reader::XmlEvent;
use xml::ParserConfig;

use crate::processors::sources::input_formats::impls::input_format_tsv::format_column_error;
use crate::processors::sources::input_formats::input_format_text::get_time_zone;
use crate::processors::sources::input_formats::input_format_text::AligningState;
use crate::processors::sources::input_formats::input_format_text::BlockBuilder;
use crate::processors::sources::input_formats::input_format_text::InputFormatTextBase;
use crate::processors::sources::input_formats::input_format_text::RowBatch;
use crate::processors::sources::input_formats::InputContext;

pub struct InputFormatXML {}

impl InputFormatXML {
    fn read_row(
        buf: &[u8],
        deserializers: &mut [TypeDeserializerImpl],
        schema: &DataSchemaRef,
        field_ends: &[usize],
        format_settings: &FormatSettings,
        path: &str,
        row_index: usize,
    ) -> Result<()> {
        let mut field_start = 0;
        for (c, deserializer) in deserializers.iter_mut().enumerate() {
            let field_end = field_ends[c];
            let col_data = &buf[field_start..field_end];
            if col_data.is_empty() {
                deserializer.de_default(format_settings);
            } else {
                let mut reader = NestedCheckpointReader::new(col_data);
                if let Err(e) = deserializer.de_text(&mut reader, format_settings) {
                    let err_msg = format_column_error(schema, c, col_data, &e.message());
                    return Err(xml_error(&err_msg, path, row_index));
                };
                reader.ignore_white_spaces().expect("must success");
                if reader.must_eof().is_err() {
                    let err_msg = format_column_error(schema, c, col_data, "bad field end");
                    return Err(xml_error(&err_msg, path, row_index));
                }
            }
            field_start = field_end;
        }
        Ok(())
    }
}

impl InputFormatTextBase for InputFormatXML {
    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Xml
    }

    fn get_format_settings(settings: &Arc<Settings>) -> Result<FormatSettings> {
        let timezone = get_time_zone(settings)?;
        Ok(FormatSettings {
            row_tag: settings.get_row_tag()?.into_bytes(),
            rowset_tag: settings.get_rowset_tag()?.into_bytes(),
            timezone,
            ..Default::default()
        })
    }

    fn default_field_delimiter() -> u8 {
        b','
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        tracing::debug!(
            "xml deserializing row batch {}, id={}, start_row={:?}, offset={}",
            batch.path,
            batch.batch_id,
            batch.start_row,
            batch.offset,
        );
        let columns = &mut builder.mutable_columns;
        let n_column = columns.len();

        let mut start = 0usize;
        let mut field_end_idx = 0usize;
        let start_row = batch.start_row.expect("must be success");
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end];
            Self::read_row(
                buf,
                columns,
                &builder.ctx.schema,
                &batch.field_ends[field_end_idx..field_end_idx + n_column],
                &builder.ctx.format_settings,
                &batch.path,
                start_row + i,
            )?;
            start = *end;
            field_end_idx += n_column;
        }
        Ok(())
    }

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>> {
        let xml_state = state.xml_reader.as_ref().expect("must be success");

        let start_row = state.rows;
        state.offset += buf.len();

        let mut buf = buf;
        let reader = ParserConfig::new().create_reader(&mut buf);

        let mut rows_to_skip = state.rows_to_skip;

        let mut output = RowBatch {
            data: vec![],
            row_ends: vec![],
            field_ends: vec![],
            path: state.path.to_string(),
            batch_id: state.batch_id,
            offset: 0,
            start_row: Some(state.rows),
        };

        let mut cols = Vec::<Vec<u8>>::with_capacity(state.num_fields);

        let mut has_key = false;
        let mut row_end = 0usize;
        for e in reader {
            if rows_to_skip != 0 {
                match e {
                    Ok(XmlEvent::EndElement { name }) => {
                        // Arrived one row end and skip.
                        if name.local_name.into_bytes().eq(&xml_state.row_tag) {
                            rows_to_skip -= 1;
                        }
                    }
                    Err(e) => {
                        return Err(xml_error(
                            e.msg(),
                            &state.path,
                            start_row + output.row_ends.len(),
                        ));
                    }
                    _ => {}
                }
            } else {
                match e {
                    Ok(XmlEvent::StartElement {
                        name, attributes, ..
                    }) => {
                        let name_byte = name.local_name.into_bytes();

                        // Column names as tags and column values as the content of these tags.
                        if attributes.is_empty() && !name_byte.eq(&xml_state.rowset_tag) {
                            has_key = true;
                        } else if !attributes.is_empty() && name_byte.eq(&xml_state.row_tag) {
                            // Column name as attributes and column values as attribute values.
                            for attr in attributes {
                                cols.push(attr.value.into_bytes());
                            }
                        } else if !attributes.is_empty() && name_byte.eq(&xml_state.field_tag) {
                            // Column names are the name attributes of <field> tags, and values are the contents of there tags.
                            if attributes.len() > 1 {
                                return Err(xml_error(
                                    &format!(
                                        "invalid field tag, expect 1 attr, but got {}",
                                        attributes.len()
                                    ),
                                    &state.path,
                                    start_row + output.row_ends.len(),
                                ));
                            }
                            has_key = true;
                        }
                    }
                    Ok(XmlEvent::EndElement { name }) => {
                        if name.local_name.into_bytes().eq(&xml_state.row_tag) {
                            // one row is end
                            let mut col_len = 0usize;
                            for col in &cols {
                                col_len += col.len();
                                output.field_ends.push(col_len);
                                output.data.extend_from_slice(col);
                            }
                            row_end += col_len;
                            output.row_ends.push(row_end);
                            cols.clear();
                        }
                    }
                    Ok(XmlEvent::Characters(v)) => {
                        if has_key {
                            cols.push(v.into_bytes());
                            has_key = false;
                        }
                    }
                    Err(e) => {
                        return Err(xml_error(
                            e.msg(),
                            &state.path,
                            start_row + output.row_ends.len(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(vec![output])
    }
}

fn xml_error(msg: &str, path: &str, row: usize) -> ErrorCode {
    let row = row + 1;
    let msg = format!("fail to parse XML {}:{} {} ", path, row, msg);

    ErrorCode::BadBytes(msg)
}

pub struct XmlReaderState {
    // In xml format, this field is represented as a row tag, e.g. <row>...</row>
    pub row_tag: Vec<u8>,
    // In xml format, this fields is represented as a row set tag, e.g.  <list> <row>...</row>, <row>...</row> </list>
    pub rowset_tag: Vec<u8>,
    pub field_tag: Vec<u8>,
}

impl XmlReaderState {
    pub fn create(ctx: &Arc<InputContext>) -> XmlReaderState {
        XmlReaderState {
            row_tag: ctx.format_settings.row_tag.clone(),
            rowset_tag: ctx.format_settings.rowset_tag.clone(),
            field_tag: vec![b'f', b'i', b'e', b'l', b'd'],
        }
    }
}

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
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FieldDecoder;
use common_formats::FieldDecoderRowBased;
use common_formats::FieldDecoderXML;
use common_formats::FileFormatOptionsExt;
use common_io::cursor_ext::*;
use common_meta_types::StageFileFormatType;
use xml::reader::XmlEvent;
use xml::EventReader;
use xml::ParserConfig;

use crate::processors::sources::input_formats::input_format_text::AligningState;
use crate::processors::sources::input_formats::input_format_text::BlockBuilder;
use crate::processors::sources::input_formats::input_format_text::InputFormatTextBase;
use crate::processors::sources::input_formats::input_format_text::RowBatch;
use crate::processors::sources::input_formats::InputContext;

pub struct InputFormatXML {}

impl InputFormatXML {
    fn read_row(
        field_decoder: &FieldDecoderXML,
        buf: &[u8],
        deserializers: &mut [TypeDeserializerImpl],
        schema: &DataSchemaRef,
        path: &str,
        row_index: usize,
    ) -> Result<()> {
        let mut raw_data: HashMap<String, Vec<u8>> = serde_json::from_reader(buf)?;

        if !field_decoder.ident_case_sensitive {
            raw_data = raw_data
                .into_iter()
                .map(|(k, v)| (k.to_lowercase(), v))
                .collect();
        }

        for (field, deserializer) in schema.fields().iter().zip(deserializers.iter_mut()) {
            let value = if field_decoder.ident_case_sensitive {
                raw_data.get(field.name())
            } else {
                raw_data.get(&field.name().to_lowercase())
            };

            if let Some(value) = value {
                let mut reader = Cursor::new(&**value);
                if reader.eof() {
                    deserializer.de_default();
                } else {
                    if let Err(e) = field_decoder.read_field(deserializer, &mut reader, true) {
                        let value_str = format!("{:?}", value);
                        let err_msg = format!("{}. column={} value={}", e, field.name(), value_str);
                        return Err(xml_error(&err_msg, path, row_index));
                    };
                    if reader.must_eof().is_err() {
                        let value_str = format!("{:?}", value);
                        let err_msg =
                            format!("bad field end. column={} value={}", field.name(), value_str);
                        return Err(xml_error(&err_msg, path, row_index));
                    }
                }
            } else {
                deserializer.de_default();
            }
        }
        Ok(())
    }
}

impl InputFormatTextBase for InputFormatXML {
    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Xml
    }

    fn create_field_decoder(options: &FileFormatOptionsExt) -> Arc<dyn FieldDecoder> {
        Arc::new(FieldDecoderXML::create(options))
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        tracing::debug!(
            "xml deserializing row batch {}, id={}, start_row={:?}, offset={}",
            batch.path,
            batch.batch_id,
            batch.start_row,
            batch.offset,
        );
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldDecoderXML>()
            .expect("must success");
        let columns = &mut builder.mutable_columns;

        let mut start = 0usize;
        let start_row = batch.start_row.expect("must be success");
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end];
            Self::read_row(
                field_decoder,
                buf,
                columns,
                &builder.ctx.schema,
                &batch.path,
                start_row + i,
            )?;
            start = *end;
        }
        Ok(())
    }

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>> {
        let xml_state = state.xml_reader.as_mut().expect("must be success");
        xml_state.put_part_xml_data(buf);

        if !xml_state.is_end() {
            return Ok(vec![]);
        }

        let binding = xml_state.get_date();
        let mut buf = binding.as_slice();
        let start_row = state.rows;
        state.offset += buf.len();

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

        let mut cols = HashMap::with_capacity(state.num_fields);

        let mut key = None;
        let mut row_end = 0usize;
        let mut has_start_row = false;
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
                        let name_byte = name.local_name.clone().into_bytes();
                        match attributes.is_empty() {
                            true => {
                                // Column names as tags and column values as the content of these tags.
                                if !name_byte.eq(&xml_state.row_tag) && has_start_row {
                                    key = Some(name.local_name);
                                } else if name_byte.eq(&xml_state.row_tag) {
                                    has_start_row = true;
                                }
                            }
                            false => {
                                // Column name as attributes and column values as attribute values.
                                if name_byte.eq(&xml_state.row_tag) {
                                    for attr in attributes {
                                        let key = attr.name.local_name;
                                        let value = attr.value;
                                        cols.insert(key, value.into_bytes());
                                    }
                                } else if name_byte.eq(&xml_state.field_tag) {
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
                                    let attr = attributes.get(0).unwrap();
                                    key = Some(attr.value.clone());
                                }
                            }
                        }
                    }
                    Ok(XmlEvent::EndElement { name }) => {
                        if name.local_name.into_bytes().eq(&xml_state.row_tag) {
                            let cols_bytes = serde_json::to_vec(&cols)?;

                            output.data.extend_from_slice(&cols_bytes);
                            row_end += cols_bytes.len();
                            output.row_ends.push(row_end);
                            cols.clear();
                            has_start_row = false;
                        }
                    }
                    Ok(XmlEvent::Characters(v)) => {
                        if let Some(key) = key.take() {
                            cols.insert(key, v.into_bytes());
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
    row_tag: Vec<u8>,
    field_tag: Vec<u8>,
    end_tag: Vec<u8>,
    data: Vec<u8>,
    is_end: bool,
}

impl XmlReaderState {
    pub fn create(ctx: &Arc<InputContext>) -> XmlReaderState {
        XmlReaderState {
            row_tag: ctx.format_options.stage.row_tag.as_bytes().to_vec(),
            field_tag: vec![b'f', b'i', b'e', b'l', b'd'],
            end_tag: vec![],
            data: vec![],
            is_end: false,
        }
    }

    fn is_end(&self) -> bool {
        self.is_end
    }

    fn get_date(&self) -> Vec<u8> {
        self.data.clone()
    }

    fn compare_end_tag(&self, data: &[u8]) -> bool {
        let mut n_data = data.len() - 1;
        while data[n_data] == b'\r' || data[n_data] == b'\n' {
            n_data -= 1;
        }

        let n_end_tag = self.end_tag.len();
        n_data > n_end_tag && self.end_tag.eq(&data[n_data - n_end_tag..n_data])
    }

    fn put_part_xml_data(&mut self, input: &[u8]) {
        let buf = Cursor::new(input);
        let mut reader = EventReader::new(buf).into_iter();
        let mut tmp_end_tag = vec![];
        if self.end_tag.is_empty() {
            while let Some(Ok(event)) = reader.next() {
                if let XmlEvent::StartElement { name, .. } = event {
                    let name_byte = name.local_name.into_bytes();
                    if !name_byte.eq(&self.row_tag) && tmp_end_tag.is_empty() {
                        // maybe column name.
                        tmp_end_tag = name_byte.clone();
                    }
                    if name_byte.eq(&self.row_tag)
                        && self.end_tag.is_empty()
                        && !tmp_end_tag.is_empty()
                    {
                        self.end_tag = tmp_end_tag;
                        if self.compare_end_tag(input) {
                            self.is_end = true;
                        }
                        break;
                    }
                }
            }
        } else if self.compare_end_tag(input) {
            self.is_end = true;
        }
        self.data.extend_from_slice(input);
    }
}

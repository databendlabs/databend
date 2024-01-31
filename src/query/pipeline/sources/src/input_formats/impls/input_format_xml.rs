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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::TableSchemaRef;
use databend_common_formats::FieldDecoder;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_formats::SeparatedTextDecoder;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::XmlFileFormatParams;
use databend_common_storage::FileParseError;
use xml::reader::XmlEvent;
use xml::ParserConfig;

use crate::input_formats::error_utils::truncate_column_data;
use crate::input_formats::AligningStateTextBased;
use crate::input_formats::BlockBuilder;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormatTextBase;
use crate::input_formats::RowBatch;
use crate::input_formats::SplitInfo;

pub struct InputFormatXML {}

impl InputFormatXML {
    pub fn create() -> Self {
        Self {}
    }
    fn read_row(
        field_decoder: &SeparatedTextDecoder,
        row_data: &mut HashMap<String, Vec<u8>>,
        columns: &mut [ColumnBuilder],
        schema: &TableSchemaRef,
        ident_case_sensitive: bool,
    ) -> std::result::Result<(), FileParseError> {
        let raw_data = if !ident_case_sensitive {
            row_data
                .drain()
                .map(|(k, v)| (k.to_lowercase(), v))
                .collect()
        } else {
            row_data.clone()
        };

        for ((column_index, field), column) in
            schema.fields().iter().enumerate().zip(columns.iter_mut())
        {
            let value = if ident_case_sensitive {
                raw_data.get(field.name())
            } else {
                raw_data.get(&field.name().to_lowercase())
            };
            if let Some(value) = value {
                if value.is_empty() {
                    column.push_default();
                } else if let Err(e) = field_decoder.read_field(column, value) {
                    return Err(FileParseError::ColumnDecodeError {
                        column_index,
                        column_name: field.name().to_string(),
                        column_type: field.data_type.to_string(),
                        decode_error: e.message(),
                        column_data: truncate_column_data(
                            String::from_utf8_lossy(value).to_string(),
                        ),
                    });
                }
            } else {
                column.push_default();
            }
        }
        Ok(())
    }
}

pub struct AligningStateWholeFile {
    #[allow(unused)]
    split_info: Arc<SplitInfo>,
    bufs: Vec<Vec<u8>>,
}

impl AligningStateWholeFile {
    fn try_create(_ctx: &Arc<InputContext>, split_info: &Arc<SplitInfo>) -> Result<Self> {
        Ok(Self {
            split_info: split_info.clone(),
            bufs: vec![],
        })
    }
}

impl AligningStateTextBased for AligningStateWholeFile {
    fn align(&mut self, buf: &[u8]) -> Result<Vec<RowBatch>> {
        self.bufs.push(buf.to_vec());
        Ok(vec![])
    }

    fn align_flush(&mut self) -> Result<Vec<RowBatch>> {
        let data = self.bufs.concat();

        Ok(vec![RowBatch {
            data,
            row_ends: vec![],
            field_ends: vec![],
            batch_id: 0,
            split_info: self.split_info.clone(),
            start_offset_in_split: 0,
            start_row_in_split: 0,
            start_row_of_split: Some(0),
            num_fields: vec![],
        }])
    }
}

impl InputFormatTextBase for InputFormatXML {
    type AligningState = AligningStateWholeFile;

    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Xml
    }

    fn create_field_decoder(
        params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder> {
        Arc::new(SeparatedTextDecoder::create_xml(
            XmlFileFormatParams::downcast_unchecked(params),
            options,
        ))
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        AligningStateWholeFile::try_create(ctx, split_info)
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<SeparatedTextDecoder>()
            .expect("must success");
        let columns = &mut builder.mutable_columns;

        let path = &batch.split_info.file.path;
        let xml_params = XmlFileFormatParams::downcast_unchecked(&builder.ctx.file_format_params);
        let row_tag = xml_params.row_tag.as_bytes().to_vec();
        let field_tag = vec![b'f', b'i', b'e', b'l', b'd'];

        let mut buf = Cursor::new(&batch.data);
        let num_fields = builder.ctx.schema.fields().len();
        let reader = ParserConfig::new().create_reader(&mut buf);

        let mut rows_to_skip = 0;

        let mut cols = HashMap::with_capacity(num_fields);

        let mut key = None;
        let mut has_start_row = false;
        // for deal with on_error mode
        let mut num_rows = 0usize;

        for e in reader {
            if rows_to_skip != 0 {
                match e {
                    Ok(XmlEvent::EndElement { name }) => {
                        // Arrived one row end and skip.
                        if name.local_name.into_bytes().eq(&row_tag) {
                            rows_to_skip -= 1;
                        }
                    }
                    Err(e) => {
                        return Err(xml_error(e.msg(), path, num_rows));
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
                                if !name_byte.eq(&row_tag) && has_start_row {
                                    key = Some(name.local_name);
                                } else if name_byte.eq(&row_tag) {
                                    has_start_row = true;
                                }
                            }
                            false => {
                                // Column name as attributes and column values as attribute values.
                                if name_byte.eq(&row_tag) {
                                    for attr in attributes {
                                        let key = attr.name.local_name;
                                        let value = attr.value;
                                        cols.insert(key, value.into_bytes());
                                    }
                                } else if name_byte.eq(&field_tag) {
                                    if attributes.len() > 1 {
                                        return Err(xml_error(
                                            &format!(
                                                "invalid field tag, expect 1 attr, but got {}",
                                                attributes.len()
                                            ),
                                            path,
                                            num_rows,
                                        ));
                                    }
                                    let attr = attributes.first().unwrap();
                                    key = Some(attr.value.clone());
                                }
                            }
                        }
                    }
                    Ok(XmlEvent::EndElement { name }) => {
                        if name.local_name.into_bytes().eq(&row_tag) {
                            if let Err(e) = Self::read_row(
                                field_decoder,
                                &mut cols,
                                columns,
                                &builder.ctx.schema,
                                builder.ident_case_sensitive,
                            ) {
                                builder
                                    .ctx
                                    .on_error(
                                        e,
                                        Some((columns, builder.num_rows)),
                                        &mut builder.file_status,
                                        path,
                                        num_rows + batch.start_row_in_split,
                                    )
                                    .map_err(|e| xml_error(&e.message(), path, num_rows))?;
                            } else {
                                builder.num_rows += 1;
                                builder.file_status.num_rows_loaded += 1;
                            }
                            num_rows += 1;
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
                        return Err(xml_error(e.msg(), path, num_rows));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

fn xml_error(msg: &str, path: &str, row: usize) -> ErrorCode {
    let row = row + 1;
    let msg = format!("fail to parse XML {}:{} {} ", path, row, msg);

    ErrorCode::BadBytes(msg)
}

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

use std::borrow::Cow;

use bstr::ByteSlice;
use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_meta_types::StageFileFormatType;

use crate::processors::sources::input_formats::input_format_text::AligningState;
use crate::processors::sources::input_formats::input_format_text::BlockBuilder;
use crate::processors::sources::input_formats::input_format_text::InputFormatTextBase;
use crate::processors::sources::input_formats::input_format_text::RowBatch;

pub struct InputFormatNDJson {}

impl InputFormatNDJson {
    fn read_row(
        buf: &[u8],
        deserializers: &mut [TypeDeserializerImpl],
        format_settings: &FormatSettings,
        schema: &DataSchemaRef,
    ) -> Result<()> {
        let mut json: serde_json::Value = serde_json::from_reader(buf)?;
        // if it's not case_sensitive, we convert to lowercase
        if !format_settings.ident_case_sensitive {
            if let serde_json::Value::Object(x) = json {
                let y = x.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
                json = serde_json::Value::Object(y);
            }
        }

        for (f, deser) in schema.fields().iter().zip(deserializers.iter_mut()) {
            let value = if format_settings.ident_case_sensitive {
                &json[f.name().to_owned()]
            } else {
                &json[f.name().to_lowercase()]
            };

            deser.de_json(value, format_settings).map_err(|e| {
                let value_str = format!("{:?}", value);
                ErrorCode::BadBytes(format!(
                    "{}. column={} value={}",
                    e,
                    f.name(),
                    maybe_truncated(&value_str, 1024),
                ))
            })?;
        }
        Ok(())
    }
}

impl InputFormatTextBase for InputFormatNDJson {
    fn format_type() -> StageFileFormatType {
        StageFileFormatType::NdJson
    }

    fn default_field_delimiter() -> u8 {
        b','
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let start_row = batch.start_row;
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end];
            let buf = buf.trim();
            if !buf.is_empty() {
                if let Err(e) = Self::read_row(
                    buf,
                    columns,
                    &builder.ctx.format_settings,
                    &builder.ctx.schema,
                ) {
                    let row_info = if let Some(r) = start_row {
                        format!("row={},", r + i)
                    } else {
                        String::new()
                    };
                    let msg = format!(
                        "fail to parse NDJSON: {},  path={}, offset={}, {}",
                        &batch.path,
                        e,
                        batch.offset + start,
                        row_info,
                    );
                    return Err(ErrorCode::BadBytes(msg));
                }
            }
            start = *end + 1;
        }
        Ok(())
    }

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>> {
        Ok(state.align_by_record_delimiter(buf))
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

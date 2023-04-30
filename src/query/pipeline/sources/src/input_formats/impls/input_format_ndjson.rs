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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use bstr::ByteSlice;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::TableSchemaRef;
use common_formats::FieldDecoder;
use common_formats::FieldJsonAstDecoder;
use common_formats::FileFormatOptionsExt;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageFileFormatType;
use common_pipeline_core::InputError;

use crate::input_formats::AligningStateRowDelimiter;
use crate::input_formats::BlockBuilder;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormatTextBase;
use crate::input_formats::RowBatch;
use crate::input_formats::SplitInfo;

pub struct InputFormatNDJson {}

impl InputFormatNDJson {
    pub fn create() -> Self {
        Self {}
    }
    fn read_row(
        field_decoder: &FieldJsonAstDecoder,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        schema: &TableSchemaRef,
    ) -> Result<()> {
        let mut json: serde_json::Value = serde_json::from_reader(buf)?;
        // if it's not case_sensitive, we convert to lowercase
        if !field_decoder.ident_case_sensitive {
            if let serde_json::Value::Object(x) = json {
                let y = x.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
                json = serde_json::Value::Object(y);
            }
        }

        for (f, column) in schema.fields().iter().zip(columns.iter_mut()) {
            let value = if field_decoder.ident_case_sensitive {
                &json[f.name().to_owned()]
            } else {
                &json[f.name().to_lowercase()]
            };
            field_decoder.read_field(column, value).map_err(|e| {
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
    type AligningState = AligningStateRowDelimiter;

    fn format_type() -> StageFileFormatType {
        StageFileFormatType::NdJson
    }

    fn is_splittable() -> bool {
        true
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        AligningStateRowDelimiter::try_create(ctx, split_info, b'\n', 0)
    }

    fn create_field_decoder(
        _params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder> {
        Arc::new(FieldJsonAstDecoder::create(options))
    }

    fn deserialize(
        builder: &mut BlockBuilder<Self>,
        batch: RowBatch,
    ) -> Result<HashMap<u16, InputError>> {
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldJsonAstDecoder>()
            .expect("must success");

        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let mut num_rows = 0usize;
        let mut error_map: HashMap<u16, InputError> = HashMap::new();
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end];
            let buf = buf.trim();
            if !buf.is_empty() {
                if let Err(e) = Self::read_row(field_decoder, buf, columns, &builder.ctx.schema) {
                    match builder.ctx.on_error_mode {
                        OnErrorMode::Continue => {
                            Self::on_error_continue(columns, num_rows, e.clone(), &mut error_map);
                            start = *end;
                            continue;
                        }
                        OnErrorMode::AbortNum(n) => {
                            Self::on_error_abort(
                                columns,
                                num_rows,
                                n,
                                &builder.ctx.on_error_count,
                                e,
                            )
                            .map_err(|e| batch.error(&e.message(), &builder.ctx, start, i))?;

                            start = *end;
                            continue;
                        }
                        _ => return Err(batch.error(&e.message(), &builder.ctx, start, i)),
                    }
                }
            }
            start = *end;
            num_rows += 1;
        }
        Ok(error_map)
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

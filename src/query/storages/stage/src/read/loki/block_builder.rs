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

use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_formats::FieldJsonAstDecoder;
use databend_common_meta_app::principal::LokiFileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_storage::FileParseError;
use serde_json::Value;

use super::chunk::Chunk;
use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::row_based::truncate_column_data;
use crate::read::whole_file_reader::WholeFileData;

pub(super) struct BlockBuilder {
    ctx: Arc<LoadContext>,
    params: LokiFileFormatParams,
    field_decoder: FieldJsonAstDecoder,
}

impl BlockBuilder {
    pub fn create(
        ctx: Arc<LoadContext>,
        params: LokiFileFormatParams,
        case_sensitive: bool,
    ) -> Self {
        let mut field_decoder = FieldJsonAstDecoder::create(&ctx.settings, ctx.is_select);
        field_decoder.ident_case_sensitive = case_sensitive;
        Self {
            ctx,
            params,
            field_decoder,
        }
    }

    fn read_row(
        &self,
        record: &Value,
        columns: &mut [ColumnBuilder],
    ) -> std::result::Result<(), FileParseError> {
        if self.ctx.is_select {
            let Some(ColumnBuilder::Variant(column)) = columns.first_mut() else {
                return Err(FileParseError::InvalidRow {
                    format: "LOKI".to_string(),
                    message: "expected a single VARIANT column for SELECT".to_string(),
                });
            };
            jsonb::Value::from(record).write_to_vec(&mut column.data);
            column.commit_row();
            return Ok(());
        }
        for (column_index, (field, column)) in
            self.ctx.schema.fields().iter().zip(columns).enumerate()
        {
            let name = if self.field_decoder.ident_case_sensitive {
                field.name().clone()
            } else {
                field.name().to_lowercase()
            };
            match record.get(&name) {
                Some(value) => self
                    .field_decoder
                    .read_field_with_data_type(column, value, field.data_type())
                    .map_err(|e| FileParseError::ColumnDecodeError {
                        column_index,
                        column_name: field.name().clone(),
                        column_type: field.data_type().to_string(),
                        decode_error: e.to_string(),
                        column_data: truncate_column_data(value.to_string()),
                    })?,
                None => match self.params.missing_field_as {
                    NullAs::FieldDefault => self.ctx.push_default_value(column, column_index)?,
                    NullAs::Null if field.is_nullable_or_null() => column.push_default(),
                    _ => {
                        return Err(FileParseError::ColumnMissingError {
                            column_index,
                            column_name: field.name().clone(),
                            column_type: field.data_type().to_string(),
                            advice: format!(
                                "LOKI MISSING_FIELD_AS={}",
                                self.params.missing_field_as
                            ),
                        });
                    }
                },
            }
        }
        Ok(())
    }
}

impl AccumulatingTransform for BlockBuilder {
    const NAME: &'static str = "LokiBlockBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let file = data
            .get_owned_meta()
            .and_then(WholeFileData::downcast_from)
            .ok_or_else(|| ErrorCode::Internal("missing Loki file data"))?;
        let bad_chunk = |message: String| {
            ErrorCode::BadBytes(format!("Invalid LOKI chunk '{}': {message}", file.path))
        };
        let chunk = Chunk::parse(&file.data).map_err(&bad_chunk)?;
        // Keep state and blocks local to the file. Never publish partial data/status for
        // a structurally corrupt chunk, even when row conversion uses ON_ERROR=CONTINUE.
        let mut state = BlockBuilderState::create(self.ctx.clone());
        state.file_path = file.path.clone();
        state.file_content_key = file.content_key.clone();
        state.file_last_modified = file.last_modified;
        let mut record = serde_json::json!({
            "timestamp_ns": 0,
            "message": "",
            "labels": chunk.labels,
            "tenant_id": chunk.tenant_id,
            "structured_metadata": {},
        });
        let mut output = Vec::new();
        let mut row_id = 0;
        for block_index in 0..chunk.block_count() {
            for entry in chunk.decode_block(block_index).map_err(&bad_chunk)? {
                let entry = entry.map_err(&bad_chunk)?;
                record["timestamp_ns"] = entry.timestamp_ns.into();
                record["message"] = entry.message.into();
                record["structured_metadata"] = Value::Object(entry.structured_metadata);
                match self.read_row(&record, &mut state.column_builders) {
                    Ok(()) => {
                        state.add_row(row_id);
                        state.add_internals_columns_batch(1);
                        output.extend(state.try_flush_block_by_memory(&self.ctx)?);
                    }
                    Err(error) => self.ctx.error_handler.on_error(
                        error.with_row(row_id),
                        Some((&mut state.column_builders, state.num_rows)),
                        &mut state.file_status,
                        &file.path,
                    )?,
                }
                row_id += 1;
            }
        }
        output.extend(state.flush_block(true)?);
        state.flush_status(&self.ctx.table_context)?;
        self.ctx
            .table_context
            .get_scan_progress()
            .incr(&ProgressValues {
                rows: row_id,
                bytes: 0,
            });
        Ok(output)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        Ok(vec![])
    }
}

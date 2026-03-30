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

use std::time::Duration;
use std::time::Instant;

use arrow_ipc::CompressionType;
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::writer::StreamWriter;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_formats::field_encoder::FieldEncoderToString;
use databend_common_io::prelude::OutputFormatSettings;
use log::info;
use serde::ser::SerializeSeq;

fn data_is_null(column: &Column, row_index: usize) -> bool {
    match column {
        Column::Null { .. } => true,
        Column::Nullable(box inner) => !inner.validity.get_bit(row_index),
        _ => false,
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlocksCollector {
    // Vec<Column> for a Block
    columns: Vec<(Vec<Column>, usize)>,
}

impl BlocksCollector {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn append_columns(&mut self, columns: Vec<Column>, num_rows: usize) {
        self.columns.push((columns, num_rows));
    }

    pub fn append_block(&mut self, block: DataBlock) {
        if block.is_empty() {
            return;
        }
        let columns = block.columns().iter().map(BlockEntry::to_column).collect();
        let num_rows = block.num_rows();
        self.append_columns(columns, num_rows);
    }

    pub fn num_rows(&self) -> usize {
        self.columns.iter().map(|(_, num_rows)| *num_rows).sum()
    }

    pub fn into_serializer(self, format: OutputFormatSettings) -> BlocksSerializer {
        BlocksSerializer {
            columns: self.columns,
            format: Some(format),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlocksSerializer {
    // Vec<Column> for a Block
    columns: Vec<(Vec<Column>, usize)>,
    format: Option<OutputFormatSettings>,
}

impl BlocksSerializer {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            format: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn num_rows(&self) -> usize {
        self.columns.iter().map(|(_, num_rows)| *num_rows).sum()
    }

    pub fn to_arrow_ipc(
        &self,
        data_schema: &DataSchema,
        ext_meta: Vec<(String, String)>,
    ) -> Result<Vec<u8>> {
        let mut schema = arrow_schema::Schema::from(data_schema);
        schema.metadata.extend(ext_meta);

        let mut buf = Vec::new();
        let opts = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let mut writer = StreamWriter::try_new_with_options(&mut buf, &schema, opts)?;

        for (block, _) in &self.columns {
            let block = DataBlock::new_from_columns(block.clone());
            let batch = block.to_record_batch_with_dataschema(data_schema)?;
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(buf)
    }
}

impl serde::Serialize for BlocksSerializer {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.num_rows()))?;
        if let Some(format) = &self.format {
            let start = Instant::now();
            let encoder = FieldEncoderToString::create(format);
            for (columns, num_rows) in self.columns.iter() {
                for i in 0..*num_rows {
                    serialize_seq.serialize_element(&RowSerializer {
                        format,
                        data_block: columns,
                        encoder: &encoder,
                        row_index: i,
                    })?
                }
            }
            let duration = Instant::now().duration_since(start);
            if duration >= Duration::from_secs(3) {
                info!(
                    "[SLOW] http handler serialize {} rows using {} secs",
                    self.num_rows(),
                    duration.as_secs_f64()
                );
            }
        }
        serialize_seq.end()
    }
}

struct RowSerializer<'a> {
    format: &'a OutputFormatSettings,
    data_block: &'a [Column],
    encoder: &'a FieldEncoderToString,
    row_index: usize,
}

impl serde::Serialize for RowSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.data_block.len()))?;

        for column in self.data_block.iter() {
            if !self.format.format_null_as_str && data_is_null(column, self.row_index) {
                serialize_seq.serialize_element(&None::<String>)?;
                continue;
            }
            let string = self
                .encoder
                .encode(column, self.row_index)
                .map_err(serde::ser::Error::custom)?;
            serialize_seq.serialize_element(&string)?;
        }
        serialize_seq.end()
    }
}

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
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::read_columns_many;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::read_metadata;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::FormatFactory;
use crate::formats::InputFormat;
use crate::formats::InputState;

pub struct ParquetInputState {
    pub memory: Vec<u8>,
}

impl InputState for ParquetInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct ParquetInputFormat {
    schema: DataSchemaRef,
    arrow_table_schema: ArrowSchema,
}

impl ParquetInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        factory.register_input(
            "parquet",
            Box::new(
                |name: &str, schema: DataSchemaRef, _settings: FormatSettings| {
                    ParquetInputFormat::try_create(name, schema)
                },
            ),
        )
    }

    pub fn try_create(_name: &str, schema: DataSchemaRef) -> Result<Box<dyn InputFormat>> {
        let arrow_table_schema = schema.to_arrow();
        Ok(Box::new(ParquetInputFormat {
            schema,
            arrow_table_schema,
        }))
    }

    fn read_meta_data(cursor: &mut Cursor<&Vec<u8>>) -> Result<FileMetaData> {
        match read_metadata(cursor) {
            Ok(metadata) => Ok(metadata),
            Err(cause) => Err(ErrorCode::ParquetError(cause.to_string())),
        }
    }

    fn read_columns(
        fields: &[Field],
        row_group: &RowGroupMetaData,
        cursor: &mut Cursor<&Vec<u8>>,
    ) -> Result<Vec<ArrayIter<'static>>> {
        match read_columns_many(cursor, row_group, fields.to_vec(), None) {
            Ok(array) => Ok(array),
            Err(e) => Err(ErrorCode::ParquetError(e.to_string())),
        }
    }

    fn deserialize(
        num_rows: usize,
        arrays: Vec<ArrayIter<'static>>,
    ) -> Result<Chunk<Arc<dyn Array>>> {
        match RowGroupDeserializer::new(arrays, num_rows, None).next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Ok(chunk)) => Ok(chunk),
            Some(Err(e)) => Err(ErrorCode::ParquetError(e.to_string())),
        }
    }
}

impl InputFormat for ParquetInputFormat {
    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(ParquetInputState { memory: vec![] })
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>> {
        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<ParquetInputState>().unwrap();

        if state.memory.is_empty() {
            return Ok(vec![]);
        }

        let mut cursor = Cursor::new(&state.memory);
        let parquet_metadata = Self::read_meta_data(&mut cursor)?;

        let fields = &self.arrow_table_schema.fields;
        let mut data_blocks = Vec::with_capacity(parquet_metadata.row_groups.len());

        for row_group in &parquet_metadata.row_groups {
            let arrays = Self::read_columns(fields, row_group, &mut cursor)?;
            let chunk = Self::deserialize(row_group.num_rows() as usize, arrays)?;
            data_blocks.push(DataBlock::from_chunk(&self.schema, &chunk)?);
        }

        Ok(data_blocks)
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize> {
        let state = state.as_any().downcast_mut::<ParquetInputState>().unwrap();
        state.memory.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn skip_header(&self, _: &[u8], _: &mut Box<dyn InputState>) -> Result<usize> {
        Ok(0)
    }
}

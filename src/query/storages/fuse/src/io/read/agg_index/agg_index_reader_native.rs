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

use std::io::BufReader;
use std::io::Cursor;
use std::io::Seek;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::native::read as nread;
use common_exception::Result;
use common_expression::DataBlock;

use super::AggIndexReader;

impl AggIndexReader {
    pub fn deserialize_native_data(&self, data: &[u8]) -> Result<DataBlock> {
        let mut reader = Cursor::new(data);
        let schema = nread::reader::infer_schema(&mut reader)?;
        let mut metas = nread::reader::read_meta(&mut reader)?;
        let schema_descriptor = to_parquet_schema(&schema)?;
        let mut leaves = schema_descriptor.columns().to_vec();

        let mut arrays = vec![];
        for field in schema.fields.iter() {
            let n = pread::n_columns(&field.data_type);
            let curr_metas = metas.drain(..n).collect::<Vec<_>>();
            let curr_leaves = leaves.drain(..n).collect::<Vec<_>>();
            let mut pages = Vec::with_capacity(n);
            let mut readers = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                pages.push(curr_meta.pages.clone());
                let mut reader = Cursor::new(data);
                reader.seek(std::io::SeekFrom::Start(curr_meta.offset))?;
                let buffer_size = curr_meta.total_len() as usize;
                let reader = BufReader::with_capacity(buffer_size, reader);
                readers.push(reader);
            }
            let is_nested = !nread::reader::is_primitive(field.data_type());
            let array = nread::batch_read::batch_read_array(
                readers,
                curr_leaves,
                field.clone(),
                is_nested,
                pages,
            )?;
            arrays.push(array);
        }
        let chunk = Chunk::new(arrays);
        let block = DataBlock::from_arrow_chunk(&chunk, &self.schema)?;

        // Remove unused columns.
        let block = DataBlock::resort(
            block,
            &self.schema,
            &self.reader.projected_schema.as_ref().into(),
        )?;

        self.apply_agg_info(block)
    }
}

// Copyright 2021 Datafuse Labs.
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

use std::fs::File;

use common_arrow::arrow::io::parquet::write::write_file;
use common_arrow::arrow::io::parquet::write::Compression;
use common_arrow::arrow::io::parquet::write::Encoding;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::Version;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;

// Used to create test parquet files from blocks.
pub struct ParquetTestData {}

impl ParquetTestData {
    pub fn create() -> Self {
        ParquetTestData {}
    }

    pub fn write_parquet(&self, path: &str) {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new_nullable("name", Vu8::to_data_type()),
            DataField::new("age", i32::to_data_type()),
        ]);

        let block1 = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec!["jack", "ace", "bohu"]),
            Series::from_data(vec![11, 6, 24]),
        ]);

        let block2 = DataBlock::create(schema, vec![
            Series::from_data(vec!["xjack", "xace", "xbohu"]),
            Series::from_data(vec![11, 6, 24]),
        ]);
        self.write_to_parquet(path, &[block1, block2]);
    }

    pub fn write_to_parquet(&self, path: &str, blocks: &[DataBlock]) {
        let schema = blocks[0].schema().to_arrow();

        let options = WriteOptions {
            write_statistics: true,
            compression: Compression::Uncompressed,
            version: Version::V2,
        };

        let mut batches = vec![];
        let mut encodings = vec![];
        for block in blocks {
            batches.push(Ok(RecordBatch::try_from(block.clone()).unwrap()));
            encodings.push(Encoding::Plain);
        }

        let row_groups =
            RowGroupIterator::try_new(batches.into_iter(), &schema, options, encodings).unwrap();

        let mut file = File::create(path).unwrap();
        let parquet_schema = row_groups.parquet_schema().clone();
        write_file(
            &mut file,
            row_groups,
            &schema,
            parquet_schema,
            options,
            None,
        )
        .unwrap();
    }
}

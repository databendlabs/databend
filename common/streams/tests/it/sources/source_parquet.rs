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

use std::fs::File;

use common_base::tokio;
use common_datablocks::assert_blocks_eq;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::ParquetSourceBuilder;
use common_streams::Source;
use futures::io::BufReader;
use opendal::services::fs;
use opendal::Operator;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_source_parquet() -> Result<()> {
    use common_datavalues::prelude::*;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let arrow_schema = schema.to_arrow();

    use common_arrow::arrow::io::parquet::write::*;
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Lz4, // let's begin with lz4
        version: Version::V2,
    };

    let col_a = Series::from_data(vec![1i8, 1, 2, 1, 2, 3]);
    let col_b = Series::from_data(vec!["1", "1", "2", "1", "2", "3"]);
    let sample_block = DataBlock::create(schema.clone(), vec![col_a, col_b]);

    use common_arrow::arrow::chunk::Chunk;
    let batch = Chunk::try_from(sample_block)?;
    use common_arrow::parquet::encoding::Encoding;
    let encodings = std::iter::repeat(Encoding::Plain)
        .take(arrow_schema.fields.len())
        .collect::<Vec<_>>();

    let page_nums_expects = 3;
    let name = "test-parquet";
    let dir = tempfile::tempdir().unwrap();

    // write test parquet
    // write test parquet
    let len = {
        let rg_iter = std::iter::repeat(batch).map(Ok).take(page_nums_expects);
        let row_groups = RowGroupIterator::try_new(rg_iter, &arrow_schema, options, encodings)?;
        let path = dir.path().join(name);
        let mut writer = File::create(path).unwrap();

        common_arrow::write_parquet_file(&mut writer, row_groups, arrow_schema, options)
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?
    };

    let local = Operator::new(
        fs::Backend::build()
            .root(dir.path().to_str().unwrap())
            .finish()
            .await
            .unwrap(),
    );
    let stream = local.object(name).limited_reader(len);
    let stream = BufReader::with_capacity(4 * 1024 * 1024, stream);

    let default_proj = (0..schema.fields().len())
        .into_iter()
        .collect::<Vec<usize>>();

    let mut builder = ParquetSourceBuilder::create(schema);
    builder.projection(default_proj);

    let mut page_nums = 0;
    let mut parquet_source = builder.build(stream).unwrap();
    // expects `page_nums_expects` blocks, and
    while let Some(block) = parquet_source.read().await? {
        page_nums += 1;
        // for each block, the content is the same of `sample_block`
        assert_blocks_eq(
            vec![
                "+---+---+",
                "| a | b |",
                "+---+---+",
                "| 1 | 1 |",
                "| 1 | 1 |",
                "| 2 | 2 |",
                "| 1 | 1 |",
                "| 2 | 2 |",
                "| 3 | 3 |",
                "+---+---+",
            ],
            &[block],
        );
    }

    assert_eq!(page_nums_expects, page_nums);
    Ok(())
}

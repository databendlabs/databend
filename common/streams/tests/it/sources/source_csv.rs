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
use std::io::Write;

use common_base::tokio;
use common_datablocks::assert_blocks_eq;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_streams::CsvSourceBuilder;
use common_streams::Source;
use opendal::services::fs;
use opendal::Operator;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_csv_delimiter() -> Result<()> {
    for field_delimiter in [",", "\t", "#"] {
        for record_delimiter in ["\n", "\r", "~"] {
            let dir = tempfile::tempdir().unwrap();
            let name = "my-temporary-note.txt";
            let file_path = dir.path().join(name);
            let mut file = File::create(file_path).unwrap();

            write!(
                file,
                "1{}\"1\"{}1.11{}2{}\"2\"{}2{}3{}\"3-'3'-3\"{}3\"{}",
                field_delimiter,
                field_delimiter,
                record_delimiter,
                field_delimiter,
                field_delimiter,
                record_delimiter,
                field_delimiter,
                field_delimiter,
                record_delimiter,
            )
            .unwrap();

            let schema = DataSchemaRefExt::create(vec![
                DataField::new("a", i8::to_data_type()),
                DataField::new("b", Vu8::to_data_type()),
                DataField::new("c", f64::to_data_type()),
            ]);

            let local = Operator::new(
                fs::Backend::build()
                    .root(dir.path().to_str().unwrap())
                    .finish()
                    .await
                    .unwrap(),
            );

            let mut builder = CsvSourceBuilder::create(schema);
            builder.skip_header(0);
            builder.field_delimiter(field_delimiter);
            builder.record_delimiter(record_delimiter);
            builder.block_size(10);

            let reader = local.object(name).reader().await?;
            let mut csv_source = builder.build(reader)?;
            let block = csv_source.read().await?.unwrap();
            assert_blocks_eq(
                vec![
                    "+---+---------+------+",
                    "| a | b       | c    |",
                    "+---+---------+------+",
                    "| 1 | 1       | 1.11 |",
                    "| 2 | 2       | 2    |",
                    "| 3 | 3-'3'-3 | 3    |",
                    "+---+---------+------+",
                ],
                &[block],
            );

            let block = csv_source.read().await?;
            assert!(block.is_none());

            drop(file);
            dir.close().unwrap();
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_csv_text() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let name = "my-temporary-note.txt";
    let file_path = dir.path().join(name);
    let mut file = File::create(file_path).unwrap();

    write!(
        file,
        r#"1,'Beijing',100
2,'Shanghai',80
3,'Guangzhou',60
4,'Shenzhen',70
5,'Shenzhen',55
6,'Beijing',99"#
    )
    .unwrap();

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
        DataField::new("c", f64::to_data_type()),
    ]);

    let local = Operator::new(
        fs::Backend::build()
            .root(dir.path().to_str().unwrap())
            .finish()
            .await
            .unwrap(),
    );

    let mut builder = CsvSourceBuilder::create(schema);
    builder.skip_header(0);
    builder.field_delimiter(",");
    builder.record_delimiter("\n");
    builder.block_size(10);

    let reader = local.object(name).reader().await?;
    let mut csv_source = builder.build(reader)?;

    let block = csv_source.read().await?.unwrap();
    assert_blocks_eq(
        vec![
            "+---+-------------+-----+",
            "| a | b           | c   |",
            "+---+-------------+-----+",
            "| 1 | 'Beijing'   | 100 |",
            "| 2 | 'Shanghai'  | 80  |",
            "| 3 | 'Guangzhou' | 60  |",
            "| 4 | 'Shenzhen'  | 70  |",
            "| 5 | 'Shenzhen'  | 55  |",
            "| 6 | 'Beijing'   | 99  |",
            "+---+-------------+-----+",
        ],
        &[block],
    );

    let block = csv_source.read().await?;
    assert!(block.is_none());

    drop(file);
    dir.close().unwrap();

    Ok(())
}

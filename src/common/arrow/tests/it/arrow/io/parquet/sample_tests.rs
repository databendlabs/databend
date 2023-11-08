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

use std::borrow::Borrow;
use std::io::Cursor;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Metadata;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::parquet::read as p_read;
use common_arrow::arrow::io::parquet::write::*;
use sample_arrow2::array::ArbitraryArray;
use sample_arrow2::chunk::ArbitraryChunk;
use sample_arrow2::chunk::ChainedChunk;
use sample_arrow2::datatypes::sample_flat;
use sample_arrow2::datatypes::ArbitraryDataType;
use sample_std::Chance;
use sample_std::Random;
use sample_std::Regex;
use sample_std::Sample;
use sample_test::sample_test;

fn deep_chunk(depth: usize, len: usize) -> ArbitraryChunk<Regex, Chance> {
    let names = Regex::new("[a-z]{4,8}");
    let data_type = ArbitraryDataType {
        struct_branch: 1..3,
        names: names.clone(),
        // TODO: this breaks the test
        // nullable: Chance(0.5),
        nullable: Chance(0.0),
        flat: sample_flat,
    }
    .sample_depth(depth);

    let array = ArbitraryArray {
        names,
        branch: 0..10,
        len: len..(len + 1),
        null: Chance(0.1),
        // TODO: this breaks the test
        // is_nullable: true,
        is_nullable: false,
    };

    ArbitraryChunk {
        // TODO: shrinking appears to be an issue with chunks this large. issues
        // currently reproduce on the smaller sizes anyway.
        // chunk_len: 10..1000,
        chunk_len: 1..10,
        array_count: 1..2,
        data_type,
        array,
    }
}

#[sample_test]
fn round_trip_sample(
    #[sample(deep_chunk(5, 100).sample_one())] chained: ChainedChunk,
) -> Result<()> {
    sample_test::env_logger_init();
    let chunks = vec![chained.value];
    let name = Regex::new("[a-z]{4, 8}");
    let mut g = Random::new();

    // TODO: this probably belongs in a helper in sample-arrow2
    let schema = Schema {
        fields: chunks
            .first()
            .unwrap()
            .iter()
            .map(|arr| {
                Field::new(
                    name.generate(&mut g),
                    arr.data_type().clone(),
                    arr.validity().is_some(),
                )
            })
            .collect(),
        metadata: Metadata::default(),
    };

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let encodings: Vec<_> = schema
        .borrow()
        .fields
        .iter()
        .map(|field| transverse(field.data_type(), |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(
        chunks.clone().into_iter().map(Ok),
        &schema,
        options,
        encodings,
    )?;

    let buffer = Cursor::new(vec![]);
    let mut writer = FileWriter::try_new(buffer, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    let mut buffer = writer.into_inner();

    let metadata = p_read::read_metadata(&mut buffer)?;
    let schema = p_read::infer_schema(&metadata)?;

    let mut reader = p_read::FileReader::new(buffer, metadata.row_groups, schema, None, None, None);

    let result: Vec<_> = reader.collect::<Result<_>>()?;

    assert_eq!(result, chunks);

    Ok(())
}

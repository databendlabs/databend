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

use std::sync::Arc;

use bytes::Bytes;
use common_base::base::tokio;
use common_storages_parquet::InMemoryRowGroup;
use opendal::services::Memory;
use opendal::Operator;
use parquet::basic::ConvertedType;
use parquet::basic::Repetition;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::RowGroupMetaData;
use parquet::schema::types::*;

#[tokio::test]
async fn test_merge() {
    let data = Bytes::from(
        "
    Obama, Biden, Hillary, and Trump, four former presidents, 
    decided to team up for a soccer match. They hired a coach to guide them
    The coach asked Obama, What position are you good at?
    Obama replied, I'm good at organizing the midfield, skilled in passing and
    controlling the pace of the game
    ",
    );

    // prepare data
    let builder = Memory::default();
    let path = "/tmp/test/merged";
    let op = Operator::new(builder).unwrap().finish();
    let blocking_op = op.blocking();
    blocking_op.write(path, data).unwrap();
    let mut fields = vec![];

    // prepare group_meta
    let inta = Type::primitive_type_builder("a", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .with_converted_type(ConvertedType::INT_32)
        .build()
        .unwrap();
    fields.push(Arc::new(inta));

    let schema = Type::group_type_builder("schema")
        .with_repetition(Repetition::REPEATED)
        .with_fields(fields)
        .build()
        .unwrap();
    let descr = SchemaDescriptor::new(Arc::new(schema));
    let meta = RowGroupMetaData::builder(descr.into()).build().unwrap();

    // for gap=0;
    let gap0 = InMemoryRowGroup::new(path, op.clone(), &meta, None, 0, 0);

    // for gap=10
    let gap10 = InMemoryRowGroup::new(path, op, &meta, None, 10, 200);
    let ranges = vec![(1..10), (15..30), (40..50)];
    let (gap0_chunks, gap0_merged) = gap0.get_ranges(&ranges.to_vec()).await.unwrap();
    let (gap10_chunks, gap10_merged) = gap10.get_ranges(&ranges.to_vec()).await.unwrap();
    // gap=0 no merged
    assert!(!gap0_merged);
    // gap=10  merge happend
    assert!(gap10_merged);
    // compare chunks
    for (chunk0, chunk10) in gap0_chunks.iter().zip(gap10_chunks.iter()) {
        assert_eq!(*chunk0, *chunk10);
    }
}

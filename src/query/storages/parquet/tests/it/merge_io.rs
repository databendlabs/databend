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

use arrow_array::ArrayRef;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_base::base::tokio;
use databend_common_storages_parquet::InMemoryRowGroup;
use opendal::services::Memory;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Repetition;
use parquet::file::metadata::RowGroupMetaData;
use parquet::schema::types::*;
use rand::Rng;

#[tokio::test]
async fn test_merge() {
    // Set up random number generator
    let mut rng = rand::thread_rng();

    // Define the number of rows and columns in the Parquet file
    let num_rows = 100;
    let num_cols = rng.gen_range(2..10);

    // Generate random column names
    let mut column_names = Vec::new();
    for i in 0..num_cols {
        column_names.push(format!("column_{}", i));
    }

    // Define the schema for the Parquet file
    let mut fields: Vec<Field> = Vec::new();
    for name in &column_names {
        fields.push(Field::new(name, DataType::Int64, false));
    }
    let schema = Schema::new(fields);

    // Generate random data for each column
    let mut data = Vec::new();
    for _ in 0..num_cols {
        let values: Vec<i64> = (0..num_rows).map(|_| rng.gen_range(0..100)).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        data.push(array);
    }

    // Create a RecordBatch from the data and schema
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), data).unwrap();

    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // prepare data
    let data = Bytes::from(buf);
    let builder = Memory::default();
    let path = "/tmp/test/merged";
    let op = Operator::new(builder).unwrap().finish();
    let blocking_op = op.blocking();
    blocking_op.write(path, data).unwrap();

    let schema = Type::group_type_builder("schema")
        .with_repetition(Repetition::REPEATED)
        .build()
        .unwrap();
    let descr = SchemaDescriptor::new(Arc::new(schema));
    let meta = RowGroupMetaData::builder(descr.into()).build().unwrap();

    // for gap=0;
    let gap0 = InMemoryRowGroup::new(path, op.clone(), &meta, None, 0, 0);

    // for gap=10
    let gap10 = InMemoryRowGroup::new(path, op, &meta, None, 10, 200);
    let ranges = [(1..10), (15..30), (40..50)];
    let (gap0_chunks, gap0_merged) = gap0.get_ranges(ranges.as_ref()).await.unwrap();
    let (gap10_chunks, gap10_merged) = gap10.get_ranges(ranges.as_ref()).await.unwrap();
    // gap=0 no merged
    assert!(!gap0_merged);
    // gap=10  merge happend
    assert!(gap10_merged);
    // compare chunks
    for (chunk0, chunk10) in gap0_chunks.iter().zip(gap10_chunks.iter()) {
        assert_eq!(*chunk0, *chunk10);
    }
}

// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::path::PathBuf;
use std::sync::Arc;

use crate::contexts::Context;
use crate::datablocks::DataBlock;
use crate::datasources::{DataSource, Database, IDatabase, MemoryTable, Partition};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, Int64Array};
use crate::transforms::SourceTransform;

pub fn test_data_generate_source(datas: Vec<Vec<i64>>) -> SourceTransform {
    let schema = DataSchema::new(vec![DataField::new("a", DataType::Int64, false)]);
    let mut table = MemoryTable::create("t1", Arc::new(schema));

    let mut partitions = vec![];

    for (i, data) in datas.into_iter().enumerate() {
        let block = DataBlock::create(
            Arc::new(DataSchema::new(vec![DataField::new(
                "a",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(data))],
        );
        let part_name = format!("part-{}", i);
        partitions.push(Partition {
            name: part_name.clone(),
            version: 0,
        });
        table.add_partition(part_name.as_str(), block).unwrap();
    }

    let mut database = Database::create("default");
    database.add_table(Arc::new(table)).unwrap();
    let mut datasource = DataSource::create();
    datasource.add_database(Arc::new(database)).unwrap();

    let ctx = Context::create_ctx(Arc::new(datasource));
    SourceTransform::create(ctx, "default", "t1", partitions)
}

pub fn test_data_csv_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("c1", DataType::Utf8, false),
        DataField::new("c2", DataType::UInt32, false),
        DataField::new("c3", DataType::Int8, false),
        DataField::new("c4", DataType::Int16, false),
        DataField::new("c5", DataType::Int32, false),
        DataField::new("c6", DataType::Int64, false),
        DataField::new("c7", DataType::UInt8, false),
        DataField::new("c8", DataType::UInt16, false),
        DataField::new("c9", DataType::UInt32, false),
        DataField::new("c10", DataType::UInt64, false),
        DataField::new("c11", DataType::Float32, false),
        DataField::new("c12", DataType::Float64, false),
        DataField::new("c13", DataType::Utf8, false),
    ]))
}

pub fn test_data_csv_partitions() -> Vec<Partition> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    vec![
        Partition {
            name: format!(
                "{}/src/testdata/data/csv/part_0000_v0_0000100.csv",
                dir.display()
            ),
            version: 0,
        },
        Partition {
            name: format!(
                "{}/src/testdata/data/csv/part_0001_v0_0000000.csv",
                dir.display()
            ),
            version: 0,
        },
        Partition {
            name: format!(
                "{}/src/testdata/data/csv/part_0002_v0_0000100.csv",
                dir.display()
            ),
            version: 0,
        },
        Partition {
            name: format!(
                "{}/src/testdata/data/csv/part_0003_v0_0000001.csv",
                dir.display()
            ),
            version: 0,
        },
    ]
}

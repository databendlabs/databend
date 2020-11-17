// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::contexts::Context;
use crate::datablocks::DataBlock;
use crate::datasources::{DataSource, IDatabase, MemoryDatabase, MemoryTable, Partition};
use crate::datavalues::{DataField, DataSchema, DataType, Int64Array};
use crate::transforms::SourceTransform;

pub fn generate_source(datas: Vec<Vec<i64>>) -> SourceTransform {
    let schema = DataSchema::new(vec![DataField::new("a", DataType::Int64, false)]);
    let mut table = MemoryTable::new("t1", Arc::new(schema));

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

    let mut database = MemoryDatabase::create("default");
    database.add_table(Arc::new(table)).unwrap();
    let mut datasource = DataSource::create();
    datasource.add_database(Arc::new(database)).unwrap();

    let ctx = Context::create_ctx(Arc::new(datasource));
    SourceTransform::create(ctx, "default", "t1", partitions)
}

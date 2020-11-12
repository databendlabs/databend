// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datatypes::{DataField, DataSchema, DataType, Int64Array};
use crate::transforms::SourceTransform;

pub fn generate_source(datas: Vec<Vec<i64>>) -> SourceTransform {
    let mut blocks = vec![];
    for data in datas {
        blocks.push(DataBlock::new(
            DataSchema::new(vec![DataField::new("a", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(data))],
        ));
    }
    SourceTransform::create(blocks)
}

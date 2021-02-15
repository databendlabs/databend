// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

pub type Partitions = Vec<Partition>;

#[derive(Clone, Debug)]
pub struct Partition {
    pub name: String,
    pub version: u64,
}

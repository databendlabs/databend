// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Read;
use std::io::Seek;

use common_exception::Result;
use common_planners::PlanNode;

pub enum ReaderType {
    // Parquet file.
    Parquet,
}

pub struct IndexReader<R: Read + Seek> {
    pub reader: R,
    pub reader_type: ReaderType,
}

pub trait Index {
    // Create index from parquet reader.
    fn create_index<R: Read + Seek>(&self, reader: &mut IndexReader<R>) -> Result<()>;

    //Search parts by plan.
    fn search_index(&self, plan: &PlanNode) -> Result<()>;
}

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Read;
use std::io::Seek;

use common_arrow::parquet;
use common_planners::PlanNode;

use crate::index::IndexReader;
use crate::Index;
use crate::ReaderType;

pub struct Indexer {}

impl Indexer {
    pub fn create() -> Self {
        Indexer {}
    }
}

impl Index for Indexer {
    fn create_index<R: Read + Seek>(
        &self,
        reader: &mut IndexReader<R>,
    ) -> common_exception::Result<()> {
        match reader.reader_type {
            ReaderType::Parquet => {
                let meta = parquet::read::read_metadata(&mut reader.reader).unwrap();
                println!("{:?}", meta);
            }
        }

        Ok(())
    }

    fn search_index(&self, _plan: &PlanNode) -> common_exception::Result<()> {
        todo!()
    }
}

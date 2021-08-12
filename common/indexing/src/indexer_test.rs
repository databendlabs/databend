// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;
use std::fs::File;

use common_exception::Result;

use crate::Index;
use crate::IndexReader;
use crate::Indexer;
use crate::ReaderType;

#[test]
fn test_indexer_reader() -> Result<()> {
    let path = env::current_dir()?
        .join("../../tests/data/alltypes_plain.parquet")
        .display()
        .to_string();
    let file = File::open(path).unwrap();

    let indexer = Indexer::create();
    let mut reader = IndexReader {
        reader: file,
        reader_type: ReaderType::Parquet,
    };
    indexer.create_index(&mut reader)?;
    Ok(())
}

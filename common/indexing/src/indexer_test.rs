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
//

use std::env;
use std::fs::File;

use common_exception::Result;

use crate::Index;
use crate::IndexReader;
use crate::Indexer;
use crate::ReaderFormat;

#[test]
fn test_indexer_reader() -> Result<()> {
    let path = env::current_dir()?
        .join("../../tests/data/name_age_two_rowgroups.parquet")
        .display()
        .to_string();
    let file = File::open(path).unwrap();

    let indexer = Indexer::create();
    let mut reader = IndexReader {
        reader: file,
        format: ReaderFormat::Parquet,
    };
    indexer.create_index(&mut reader)?;
    Ok(())
}

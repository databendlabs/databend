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

use std::io::Read;
use std::io::Seek;

use common_arrow::parquet;
use common_planners::PlanNode;

use crate::index::IndexReader;
use crate::Index;
use crate::IndexSchema;
use crate::ReaderFormat;

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
    ) -> common_exception::Result<IndexSchema> {
        match reader.format {
            ReaderFormat::Parquet => {
                let meta = parquet::read::read_metadata(&mut reader.reader).unwrap();
                println!("{:?}", meta);
            }
        }

        todo!()
    }

    fn search_index(&self, _plan: &PlanNode) -> common_exception::Result<()> {
        todo!()
    }
}

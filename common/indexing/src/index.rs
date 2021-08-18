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

use common_exception::Result;
use common_planners::PlanNode;

use crate::MinMaxIndex;
use crate::SparseIndex;

pub struct IndexSchema {
    pub min_max: MinMaxIndex,
    pub sparse: SparseIndex,
}

/// The reader format, now only support Parquet file.
pub enum ReaderFormat {
    // Parquet file.
    Parquet,
}

pub struct IndexReader<R: Read + Seek> {
    pub reader: R,
    pub format: ReaderFormat,
}

pub trait Index {
    // Create index from data reader.
    fn create_index<R: Read + Seek>(&self, reader: &mut IndexReader<R>) -> Result<IndexSchema>;

    // Search parts by plan.
    fn search_index(&self, plan: &PlanNode) -> Result<()>;
}

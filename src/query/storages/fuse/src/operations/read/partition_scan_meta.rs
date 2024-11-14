// Copyright 2021 Datafuse Labs
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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;

use crate::operations::read::ReaderState;
use crate::operations::read::SourceReader;
use crate::BlockReadResult;

#[derive(Clone)]
pub struct PartitionScanMeta {
    pub partitions: Vec<PartInfoPtr>,
    pub source_reader: SourceReader,
    pub reader_state: ReaderState,
    pub num_rows: Vec<usize>,
    pub bitmaps: Vec<Option<Bitmap>>,
    pub columns: Vec<Vec<BlockEntry>>,
    pub io_results: VecDeque<BlockReadResult>,
}

impl PartitionScanMeta {
    pub fn create(partitions: Vec<PartInfoPtr>, source_reader: SourceReader) -> BlockMetaInfoPtr {
        let num_partitions = partitions.len();
        Box::new(PartitionScanMeta {
            partitions,
            source_reader,
            reader_state: ReaderState::Uninitialized,
            num_rows: vec![0; num_partitions],
            bitmaps: vec![None; num_partitions],
            columns: vec![vec![]; num_partitions],
            io_results: VecDeque::with_capacity(num_partitions),
        })
    }
}

impl Debug for PartitionScanMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("PartitionScanMeta")
            .field("partitions", &self.partitions)
            .finish()
    }
}

impl serde::Serialize for PartitionScanMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("Unimplemented serialize PartitionScanMeta")
    }
}

impl<'de> serde::Deserialize<'de> for PartitionScanMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("Unimplemented deserialize PartitionScanMeta")
    }
}

impl BlockMetaInfo for PartitionScanMeta {
    fn typetag_deserialize(&self) {
        unimplemented!("PartitionScanMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("PartitionScanMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals PartitionScanMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

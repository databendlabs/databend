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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use super::AggIndexReader;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;

impl AggIndexReader {
    pub fn deserialize_parquet_data(
        &self,
        part: PartInfoPtr,
        data: BlockReadResult,
    ) -> Result<DataBlock> {
        let columns_chunks = data.columns_chunks()?;
        let part = FuseBlockPartInfo::from_part(&part)?;
        let block = self.reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            columns_chunks,
            &part.compression,
            &part.location,
            None,
        )?;

        self.apply_agg_info(block)
    }
}

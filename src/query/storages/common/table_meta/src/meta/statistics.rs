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

use std::collections::HashMap;
use std::io::Cursor;

use databend_common_base::base::uuid::Uuid;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnId;
use databend_common_storage::MetaHLL;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::ColumnStatistics;
use crate::meta::SegmentStatistics;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;
pub type BlockHLL = HashMap<ColumnId, MetaHLL>;
pub type RawBlockHLL = Vec<u8>;

// Assigned to executors, describes that which blocks of given segment, an executor should take care of
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct BlockSlotDescription {
    // number of slots
    pub num_slots: usize,
    // index of slot that current executor should take care of:
    // let `block_index` be the index of block in segment,
    // `block_index` mod `num_slots` == `slot_index` indicates that the block should be taken care of by current executor
    // otherwise, the block should be taken care of by other executors
    pub slot: u32,
}

pub fn supported_stat_type(data_type: &DataType) -> bool {
    let inner_type = data_type.remove_nullable();
    matches!(
        inner_type,
        DataType::Number(_)
            | DataType::Date
            | DataType::Timestamp
            | DataType::String
            | DataType::Decimal(_)
    )
}

pub fn encode_column_hll(hll: &BlockHLL) -> Result<RawBlockHLL> {
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();

    let data = encode(&encoding, hll)?;
    let data_compress = compress(&compression, data)?;
    Ok(data_compress)
}

pub fn decode_column_hll(data: &RawBlockHLL) -> Result<Option<BlockHLL>> {
    if data.is_empty() {
        return Ok(None);
    }
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();
    let mut reader = Cursor::new(&data);
    let res = read_and_deserialize(&mut reader, data.len() as u64, &encoding, &compression)?;
    Ok(Some(res))
}

pub fn merge_column_hll(mut lhs: BlockHLL, rhs: BlockHLL) -> BlockHLL {
    merge_column_hll_mut(&mut lhs, &rhs);
    lhs
}

pub fn merge_column_hll_mut(lhs: &mut BlockHLL, rhs: &BlockHLL) {
    for (column_id, column_hll) in rhs.iter() {
        lhs.entry(*column_id)
            .and_modify(|hll| hll.merge(column_hll))
            .or_insert_with(|| column_hll.clone());
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BlockHLLState {
    Serialized(RawBlockHLL),
    Deserialized(BlockHLL),
}

impl BlockHLLState {
    pub fn merge_column_hll(lhs: &mut BlockHLL, rhs: &Option<BlockHLLState>) {
        if let Some(BlockHLLState::Deserialized(v)) = rhs {
            merge_column_hll_mut(lhs, v);
        }
    }

    pub fn encode_column_hll(hll: Option<BlockHLLState>) -> Result<Option<RawBlockHLL>> {
        hll.map(|h| match h {
            BlockHLLState::Deserialized(v) => encode_column_hll(&v),
            BlockHLLState::Serialized(v) => Ok(v),
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use databend_common_exception::Result;
    use databend_common_io::prelude::bincode_deserialize_from_slice;
    use databend_common_io::prelude::bincode_serialize_into_buf;

    use crate::meta::TableSnapshotV4;

    #[test]
    fn test_decode_snapshot() -> Result<()> {
        let data = [
            4, 0, 0, 0, 0, 0, 0, 0, 2, 1, 61, 2, 0, 0, 0, 0, 0, 0, 40, 181, 47, 253, 0, 88, 165,
            17, 0, 54, 35, 121, 66, 64, 141, 211, 1, 67, 137, 135, 177, 107, 36, 144, 113, 141,
            176, 227, 143, 66, 6, 250, 88, 58, 195, 96, 86, 155, 12, 24, 217, 97, 118, 20, 33, 122,
            203, 20, 57, 182, 148, 34, 196, 66, 208, 78, 82, 78, 20, 69, 195, 99, 219, 241, 103,
            232, 191, 131, 154, 72, 88, 108, 41, 177, 80, 214, 116, 219, 59, 109, 0, 98, 0, 96, 0,
            38, 205, 98, 160, 118, 47, 47, 95, 13, 57, 240, 72, 79, 166, 37, 33, 34, 14, 101, 199,
            139, 52, 129, 103, 127, 158, 147, 52, 21, 12, 151, 150, 7, 237, 174, 165, 197, 239,
            178, 182, 216, 137, 65, 135, 211, 193, 240, 112, 56, 58, 25, 14, 142, 70, 129, 198,
            230, 194, 147, 185, 232, 108, 50, 155, 16, 216, 228, 247, 158, 241, 77, 36, 21, 3, 33,
            48, 13, 80, 140, 196, 107, 2, 129, 134, 19, 23, 170, 0, 35, 96, 116, 95, 63, 142, 49,
            117, 148, 10, 90, 163, 219, 206, 220, 243, 180, 245, 58, 85, 8, 160, 134, 52, 158, 56,
            168, 90, 62, 88, 52, 72, 116, 215, 31, 178, 28, 108, 247, 182, 216, 172, 143, 94, 16,
            148, 251, 215, 151, 109, 108, 165, 250, 120, 85, 90, 40, 151, 167, 183, 62, 7, 161, 90,
            147, 133, 144, 229, 57, 175, 155, 84, 238, 65, 77, 79, 17, 248, 25, 242, 247, 44, 193,
            254, 139, 205, 250, 82, 40, 24, 29, 157, 87, 214, 103, 218, 112, 158, 30, 216, 96, 219,
            154, 183, 131, 36, 106, 105, 177, 148, 183, 163, 36, 106, 44, 244, 142, 159, 97, 252,
            170, 44, 81, 13, 18, 189, 132, 2, 127, 79, 199, 180, 198, 143, 16, 50, 139, 102, 251,
            253, 202, 151, 114, 241, 190, 146, 51, 119, 85, 247, 143, 198, 247, 8, 61, 88, 132,
            108, 157, 61, 29, 141, 39, 74, 202, 75, 79, 46, 32, 158, 36, 149, 136, 198, 73, 18, 66,
            137, 252, 249, 221, 201, 250, 76, 90, 131, 68, 151, 124, 223, 198, 108, 197, 214, 127,
            157, 117, 128, 253, 182, 51, 26, 87, 52, 182, 30, 212, 205, 90, 165, 165, 98, 170, 90,
            2, 154, 243, 23, 223, 227, 243, 139, 208, 66, 253, 161, 82, 241, 129, 77, 140, 2, 153,
            179, 174, 183, 209, 86, 247, 197, 233, 188, 102, 139, 45, 77, 107, 104, 116, 6, 123,
            94, 230, 242, 108, 104, 50, 31, 140, 135, 59, 12, 39, 35, 3, 163, 217, 144, 24, 137,
            248, 211, 158, 30, 237, 37, 0, 213, 104, 35, 109, 47, 96, 180, 121, 191, 8, 105, 52,
            217, 36, 108, 237, 150, 177, 4, 164, 77, 180, 81, 162, 141, 170, 170, 5, 84, 138, 13,
            195, 178, 13, 32, 64, 149, 144, 242, 67, 66, 101, 24, 209, 26, 63, 28, 0, 55, 67, 144,
            47, 73, 204, 178, 54, 65, 213, 158, 233, 221, 152, 200, 193, 240, 153, 88, 158, 150,
            53, 203, 176, 109, 99, 99, 28, 21, 215, 235, 205, 80, 110, 186, 186, 208, 96, 90, 136,
            8, 236, 35, 162, 77, 110, 227, 228, 16, 224, 200, 77, 137, 142, 18, 142, 57, 233, 192,
            25, 251, 48, 241, 192, 84, 1, 221, 215, 17, 91, 176, 40, 235, 154, 102,
        ];

        let val = TableSnapshotV4::from_slice(&data)?;

        let mut buffer = Cursor::new(Vec::new());
        bincode_serialize_into_buf(&mut buffer, &val).unwrap();
        let slice = buffer.get_ref().as_slice();
        let deserialized: TableSnapshotV4 = bincode_deserialize_from_slice(slice).unwrap();
        assert_eq!(val.summary, deserialized.summary);
        Ok(())
    }
}

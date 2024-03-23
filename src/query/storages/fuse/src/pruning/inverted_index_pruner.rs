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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

use crate::io::read::load_inverted_index_info;
use crate::io::read::InvertedIndexReader;

// IndexInfo records the location of a set of Index files,
// and one Index file corresponds to multiple SegmentInfo files.
// Through the inverted index in the Index file,
// we can prune the Segments and the Blocks.
// The relationship is shown in the following figure:
//
// ┌────────┐
// │Snapshot│
// └────────┘
//   |   |      ┌──────┬──────┬──────┬───┬──────┐
//   |   |      │Block1│Block2│Block3│...│BlockN│
//   |   |      └──────┴──────┴──────┴───┴──────┘
//   |   |      \          _________/
//   |   |       \        /
//   |   |       ┌────────┬────────┬────────┬───┬────────┐
//   |   +-------│Segment1│Segment2│Segment3│...│SegmentN│
//   |           └────────┴────────┴────────┴───┴────────┘
//   |           \         ________________/
//   |            \       /
//   |            ┌──────┬──────┬──────┬───┬──────┐
//   |            │Index1│Index2│Index3│...│IndexN│
//   |            └──────┴──────┴──────┴───┴──────┘
//   |            \           ___________________/
//   |             \         /
//   |             ┌─────────┐
//   +-------------│IndexInfo│
//                 └─────────┘
//
pub struct InvertedIndexPruner {
    segment_map: BTreeMap<String, Option<Vec<(usize, F32)>>>,
}

impl InvertedIndexPruner {
    #[async_backtrace::framed]
    pub async fn try_create(
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        index_info_locations: &Option<BTreeMap<String, Location>>,
    ) -> Result<Option<Arc<InvertedIndexPruner>>> {
        let inverted_index_info = push_down.as_ref().and_then(|p| p.inverted_index.as_ref());
        if let Some(inverted_index_info) = inverted_index_info {
            let index_info_loc = index_info_locations
                .as_ref()
                .and_then(|i| i.get(&inverted_index_info.index_name));
            if let Some(index_info_loc) = index_info_loc {
                let index_info =
                    load_inverted_index_info(dal.clone(), Some(index_info_loc)).await?;
                if index_info.is_none() {
                    return Err(ErrorCode::StorageNotFound(format!(
                        "read inverted index info {} failed",
                        index_info_loc.0
                    )));
                }
                let index_info = index_info.unwrap();

                let inverted_index_reader = InvertedIndexReader::try_create(
                    dal.clone(),
                    &inverted_index_info.index_schema,
                    &inverted_index_info.query_columns,
                    &index_info.indexes,
                )
                .await?;

                let segment_map =
                    inverted_index_reader.do_filter(&inverted_index_info.query_text)?;
                return Ok(Some(Arc::new(InvertedIndexPruner { segment_map })));
            }
            return Err(ErrorCode::StorageOther(format!(
                "inverted index {} not exist, run refresh inverted index first",
                inverted_index_info.index_name
            )));
        }
        Ok(None)
    }

    pub fn should_keep(&self, segment_loc: &str) -> bool {
        match self.segment_map.get(segment_loc) {
            Some(res) => res.is_some(),
            None => true,
        }
    }

    // Converts the row id in a segment to the row id in each blocks
    // by the row count in each blocks.
    pub fn should_keep_block(
        &self,
        segment_loc: &str,
        row_counts: Vec<usize>,
    ) -> Result<BTreeMap<usize, Vec<(usize, F32)>>> {
        if let Some(row_id_scores) = self.segment_map.get(segment_loc) {
            if let Some(row_id_scores) = row_id_scores.as_ref() {
                let mut j = 0;
                let mut prev_row_count = 0;
                let mut result_map = BTreeMap::new();
                for (i, row_count) in row_counts.iter().enumerate() {
                    // no rows left in the segment.
                    if j >= row_id_scores.len() {
                        continue;
                    }
                    let (row_id, _) = row_id_scores.get(j).unwrap();
                    // next row is not in current block.
                    if *row_id >= *row_count {
                        prev_row_count = *row_count;
                        continue;
                    }
                    let mut block_row_id_scores = Vec::new();
                    while j < row_id_scores.len() {
                        let (row_id, score) = row_id_scores.get(j).unwrap();
                        if *row_id >= *row_count {
                            break;
                        }
                        let block_row_id = row_id - prev_row_count;
                        block_row_id_scores.push((block_row_id, *score));
                        j += 1;
                    }
                    prev_row_count = *row_count;
                    result_map.insert(i, block_row_id_scores);
                }
                return Ok(result_map);
            }
        }

        Err(ErrorCode::StorageNotFound(format!(
            "segment {} have not build inverted index, run refresh inverted index first",
            segment_loc
        )))
    }
}

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

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

use crate::io::read::load_inverted_index_info;
use crate::io::read::InvertedIndexReader;

pub struct InvertedIndexPruner {
    segment_map: BTreeMap<String, Option<Vec<(usize, F32)>>>,
}

impl InvertedIndexPruner {
    #[async_backtrace::framed]
    pub async fn try_create(
        dal: Operator,
        inverted_index_info: &InvertedIndexInfo,
        index_info_locations: &Option<BTreeMap<String, Location>>,
    ) -> Result<Arc<InvertedIndexPruner>> {
        if let Some(index_info_locations) = index_info_locations {
            if let Some(index_info_loc) = index_info_locations.get(&inverted_index_info.index_name)
            {
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
                return Ok(Arc::new(InvertedIndexPruner { segment_map }));
            }
        }
        Err(ErrorCode::StorageOther(format!(
            "inverted index {} not exist, run refresh inverted index first",
            inverted_index_info.index_name
        )))
    }

    pub fn should_keep(&self, segment_loc: &str) -> bool {
        match self.segment_map.get(segment_loc) {
            Some(res) => res.is_some(),
            None => true,
        }
    }

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
                    if j >= row_id_scores.len() {
                        continue;
                    }
                    let (row_id, _) = row_id_scores.get(j).unwrap();
                    if *row_id < prev_row_count {
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

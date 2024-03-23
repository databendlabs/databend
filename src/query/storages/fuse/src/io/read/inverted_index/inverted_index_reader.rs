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

use std::cmp::Eq;
use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_expression::DataSchema;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_table_meta::meta::IndexSegmentInfo;
use futures_util::future::try_join_all;
use opendal::Operator;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::Index;

use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_filter;
use crate::io::write::create_tokenizer_manager;

#[derive(Clone)]
pub struct InvertedIndexReader {
    fields: Vec<Field>,
    directories: Vec<Arc<InvertedIndexDirectory>>,
}

impl InvertedIndexReader {
    pub async fn try_create(
        dal: Operator,
        schema: &DataSchema,
        query_columns: &Vec<String>,
        indexes: &BTreeMap<String, Vec<IndexSegmentInfo>>,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(query_columns.len());
        for column_name in query_columns {
            let i = schema.index_of(column_name)?;
            let field = Field::from_field_id(i as u32);
            fields.push(field);
        }

        let futs = indexes
            .iter()
            .map(|(index_loc, index_segments)| {
                load_inverted_index_filter(dal.clone(), index_loc.clone(), index_segments.clone())
            })
            .collect::<Vec<_>>();

        let directories = try_join_all(futs).await?.into_iter().collect();

        Ok(Self {
            fields,
            directories,
        })
    }

    // The Index file treats the indexed data as Document,
    // each Document has a doc id. The tantivy searcher
    // returns the matched doc ids with the query keyword.
    // Since an Index corresponds to multiple Blocks,
    // and the Blocks are added to the Index in the
    // same order as they are in the Segment.
    // We can convert the docIds to the corresponding Block
    // and the row id in the Block in the order it was added.
    // As shown in the following figure:
    //
    // ┌────┬───┬────┬─────┬───┬────┬───┬───┬────┬───┬────┐
    // │Doc0│...│DocM│DocM1│...│DocN│...│...│DocX│...│DocY│
    // └────┴───┴────┴─────┴───┴────┴───┴───┴────┴───┴────┘
    //   |        |      \         \           \        \
    //   |        |       \         \           \        \
    // ┌────┬───┬────┐     ┌────┬───┬────┐     ┌────┬───┬────┐
    // │Row0│...│RowN│ ... │Roc0│...│RowN│ ... │Row0│...│RowN│
    // └────┴───┴────┘     └────┴───┴────┘     └────┴───┴────┘
    //  \  Block1              BlockM   /       \  BlockN   /
    //   \          ___________________/         \         /
    //    \        /                              \       /
    //     Segment1                               SegmentN
    //
    #[allow(clippy::type_complexity)]
    pub fn do_filter(&self, query: &str) -> Result<BTreeMap<String, Option<Vec<(usize, F32)>>>> {
        let tokenizer_manager = create_tokenizer_manager();
        let mut segment_map = BTreeMap::new();
        for directory in &self.directories {
            let index_segments = directory.index_segments().clone();
            let num = index_segments.iter().map(|i| i.row_count).sum::<u64>() as usize;

            let directory = Arc::unwrap_or_clone(directory.clone());
            let mut index = Index::open(directory)?;
            index.set_tokenizers(tokenizer_manager.clone());
            let reader = index.reader()?;
            let searcher = reader.searcher();

            let query_parser = QueryParser::for_index(&index, self.fields.clone());
            let query = query_parser.parse_query(query)?;

            // TODO: support TopN
            let collector = TopDocs::with_limit(num);
            let docs = searcher.search(&query, &collector)?;

            let mut doc_id_scores = BinaryHeap::new();
            for (score, doc_addr) in docs {
                let doc_id = doc_addr.doc_id as usize;
                let score = F32::from(score);
                doc_id_scores.push(DocIdScore { doc_id, score });
            }

            // Converts the doc id in the index to the row id in each segment
            // by the row count in each segments.
            let mut row_count: usize = 0;
            let mut next_row_count: usize = 0;
            let mut row_id_scores = Vec::new();
            for index_segment in index_segments {
                next_row_count += index_segment.row_count as usize;
                while let Some(doc_id_score) = doc_id_scores.peek() {
                    if doc_id_score.doc_id < next_row_count {
                        let row_id = doc_id_score.doc_id - row_count;
                        row_id_scores.push((row_id, doc_id_score.score));
                        doc_id_scores.pop();
                    } else {
                        break;
                    }
                }
                let segment_row_id_scores = if !row_id_scores.is_empty() {
                    let segment_row_id_scores = std::mem::take(&mut row_id_scores);
                    Some(segment_row_id_scores)
                } else {
                    None
                };
                segment_map.insert(
                    index_segment.segment_location.clone(),
                    segment_row_id_scores,
                );
                row_count = next_row_count;
            }
        }

        Ok(segment_map)
    }
}

#[derive(Debug, Clone)]
pub struct DocIdScore {
    doc_id: usize,
    score: F32,
}

impl Ord for DocIdScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // Prioritise rows with smaller doc ids,
        // keeping the same order as segments
        other.doc_id.cmp(&self.doc_id)
    }
}

impl PartialOrd for DocIdScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DocIdScore {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
    }
}

impl Eq for DocIdScore {}

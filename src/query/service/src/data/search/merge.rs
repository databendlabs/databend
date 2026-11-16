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

use std::cmp::Ordering;

use crate::data::core::RowRef;
use crate::data::search::retriever::BlockHit;
use crate::data::search::retriever::ScoreOrder;

/// A row found by a search, ranked among the rows of every block.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RankedHit {
    pub row: RowRef,
    pub score: f32,
}

/// The hits ranked `offset..offset + limit` among `hits`, the hits of each block by its
/// position. See [`rank`].
pub fn top_hits(
    order: ScoreOrder,
    hits: Vec<(usize, Vec<BlockHit>)>,
    offset: usize,
    limit: usize,
) -> Vec<RankedHit> {
    let hits = hits
        .into_iter()
        .flat_map(|(block, hits)| {
            hits.into_iter().map(move |hit| RankedHit {
                row: RowRef {
                    block,
                    row: hit.row,
                },
                score: hit.score,
            })
        })
        .collect();
    rank(order, hits, offset, limit)
}

/// The hits ranked `offset..offset + limit` among `hits`. Ranked by score, then block
/// position, then row, so equal scores rank the same in every search.
pub fn rank(
    order: ScoreOrder,
    mut hits: Vec<RankedHit>,
    offset: usize,
    limit: usize,
) -> Vec<RankedHit> {
    let cmp = |a: &RankedHit, b: &RankedHit| -> Ordering {
        let by_score = match order {
            ScoreOrder::HigherFirst => b.score.total_cmp(&a.score),
            ScoreOrder::LowerFirst => a.score.total_cmp(&b.score),
        };
        by_score.then(a.row.cmp(&b.row))
    };
    let end = offset.saturating_add(limit);
    if hits.len() > end {
        // Only the first `end` need to be sorted.
        hits.select_nth_unstable_by(end, cmp);
        hits.truncate(end);
    }
    hits.sort_unstable_by(cmp);
    hits.into_iter().skip(offset).collect()
}

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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::data::search::RankedHit;

/// A row of a block found by a retriever, with its score.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BlockHit {
    /// The row number in the block.
    pub row: u32,
    pub score: f32,
}

/// Which scores rank first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScoreOrder {
    /// Relevance scores, like BM25.
    HigherFirst,
    /// Distances.
    LowerFirst,
}

/// Finds the best rows of each block. A retriever is built for one search, knowing how
/// many rows of a block that search needs at most; the executor merges the blocks.
///
/// A retriever whose block scores are approximate rescores the best of them across all
/// blocks before the final ranking: [`Self::rerank_candidates`] of them, by
/// [`Self::rerank`].
#[async_trait::async_trait]
pub trait Retriever: Send + Sync {
    fn order(&self) -> ScoreOrder;

    /// The best rows of `block`, at most the number the retriever was built for. An
    /// empty result skips the block.
    async fn search_block(&self, block: &BlockMeta) -> Result<Vec<BlockHit>>;

    /// The number of best hits of all blocks to rescore with [`Self::rerank`], or `None`
    /// when the scores of [`Self::search_block`] are final.
    fn rerank_candidates(&self) -> Option<usize> {
        None
    }

    /// `hits`, the best hits of all blocks, with their final scores, in any order.
    /// `blocks` are the blocks that [`RankedHit::row`] refers to by position.
    async fn rerank(
        &self,
        _blocks: &[Arc<BlockMeta>],
        hits: Vec<RankedHit>,
    ) -> Result<Vec<RankedHit>> {
        Ok(hits)
    }
}

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
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::NumberDataType;
use serde::Serialize;

use crate::data::core::RowFetcher;
use crate::data::core::TableTarget;
use crate::data::core::for_each_block;
use crate::data::core::run;
use crate::data::search::SCORE_COLUMN;
use crate::data::search::SearchRequest;
use crate::data::search::rank;
use crate::data::search::retrievers::build_retriever;
use crate::data::search::top_hits;
use crate::sessions::QueryContext;

/// The rows a search returns, with the columns of `SearchRequest::output`.
pub struct SearchOutput {
    pub schema: DataSchema,
    pub block: DataBlock,
    pub stats: SearchStats,
}

#[derive(Serialize, Debug, Clone, Copy, Default)]
pub struct SearchStats {
    /// Blocks of the table searched.
    pub blocks: usize,
    /// Blocks with a hit.
    pub matched_blocks: usize,
    pub rows: usize,
    pub elapsed_ms: f64,
}

/// Runs the search `request`.
pub async fn execute(ctx: Arc<QueryContext>, request: SearchRequest) -> Result<SearchOutput> {
    run("search", search(ctx, request)).await
}

async fn search(ctx: Arc<QueryContext>, request: SearchRequest) -> Result<SearchOutput> {
    let started = Instant::now();
    request.validate()?;
    let target = TableTarget::resolve(ctx.clone(), request.table_ref()).await?;
    let columns = request
        .output
        .iter()
        .map(String::as_str)
        .filter(|name| *name != SCORE_COLUMN)
        .collect::<Vec<_>>();
    let fetcher = RowFetcher::create(&target, &columns)?;
    let retriever = build_retriever(&target, &request.retrieve, request.candidates()).await?;

    let blocks = target.blocks().await?;
    let hits = for_each_block(&ctx, &blocks, |block| {
        let retriever = retriever.clone();
        async move { retriever.search_block(&block).await }
    })
    .await?;
    let matched_blocks = hits.iter().filter(|(_, hits)| !hits.is_empty()).count();
    let order = retriever.order();
    let ranked = match retriever.rerank_candidates() {
        None => top_hits(order, hits, request.offset, request.limit),
        Some(candidates) => {
            let candidates = top_hits(order, hits, 0, candidates);
            let reranked = retriever.rerank(&blocks, candidates).await?;
            rank(order, reranked, request.offset, request.limit)
        }
    };
    let rows = ranked.iter().map(|hit| hit.row).collect::<Vec<_>>();
    let fetched = fetcher.fetch(&blocks, &rows).await?;

    // The output columns in the requested order: the score, or the next fetched column.
    let fetched_schema = fetcher.schema();
    let mut fields = Vec::with_capacity(request.output.len());
    let mut entries = Vec::with_capacity(request.output.len());
    let mut next_fetched = 0;
    for name in &request.output {
        if name == SCORE_COLUMN {
            fields.push(DataField::new(
                SCORE_COLUMN,
                DataType::Number(NumberDataType::Float32),
            ));
            let scores = ranked.iter().map(|hit| F32::from(hit.score)).collect();
            entries.push(Float32Type::from_data(scores).into());
        } else {
            fields.push(fetched_schema.field(next_fetched).clone());
            entries.push(fetched.get_by_offset(next_fetched).clone());
            next_fetched += 1;
        }
    }
    Ok(SearchOutput {
        schema: DataSchema::new(fields),
        block: DataBlock::new(entries, ranked.len()),
        stats: SearchStats {
            blocks: blocks.len(),
            matched_blocks,
            rows: ranked.len(),
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        },
    })
}

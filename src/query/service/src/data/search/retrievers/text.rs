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

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::InvertedIndexOption;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::types::F32;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_storages_fuse::pruning::InvertedIndexPruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use serde::Deserialize;

use crate::data::core::TableTarget;
use crate::data::search::BlockHit;
use crate::data::search::Retriever;
use crate::data::search::ScoreOrder;
use crate::sessions::TableContextCachePolicy;

/// Full-text search over the inverted index of `columns`, scored by BM25 like `score()`.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TextSpec {
    /// The columns to search, all covered by one inverted index; `name^boost` multiplies
    /// the score of a column, like the first argument of `match`.
    pub columns: Vec<String>,
    /// The search query, in the syntax of the second argument of `match`.
    pub query: String,
    /// Whether every term must match (`and`) or any of them (`or`, the default).
    #[serde(default)]
    pub operator: TextOperator,
    /// Match terms within this Levenshtein distance.
    #[serde(default)]
    pub fuzziness: Option<u8>,
    /// Ignore the invalid parts of `query` instead of failing.
    #[serde(default)]
    pub lenient: bool,
}

#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextOperator {
    #[default]
    Or,
    And,
}

impl TextSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(ErrorCode::BadArguments("text search must list a column"));
        }
        // Like the `fuzziness` option of `match`.
        if self.fuzziness.is_some_and(|f| !(1..=2).contains(&f)) {
            return Err(ErrorCode::BadArguments(
                "text search fuzziness must be 1 or 2",
            ));
        }
        for column in &self.columns {
            parse_boosted_column(column)?;
        }
        Ok(())
    }
}

/// Splits `name^boost` into the column name and its boost.
pub fn parse_boosted_column(column: &str) -> Result<(&str, Option<f32>)> {
    let (name, boost) = match column.rsplit_once('^') {
        None => (column, None),
        Some((name, boost)) => {
            let boost = boost.trim().parse::<f32>().ok().filter(|b| b.is_finite());
            let Some(boost) = boost else {
                return Err(ErrorCode::BadArguments(format!(
                    "invalid boost in text search column `{column}`"
                )));
            };
            (name, Some(boost))
        }
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(ErrorCode::BadArguments(format!(
            "invalid text search column `{column}`"
        )));
    }
    Ok((name, boost))
}

/// BM25 full-text search over an inverted index: the rows and scores of
/// `WHERE match(columns, query) ORDER BY score() DESC LIMIT candidates`.
pub struct TextRetriever {
    pruner: Arc<InvertedIndexPruner>,
}

impl TextRetriever {
    pub async fn create(target: &TableTarget, spec: &TextSpec, candidates: usize) -> Result<Self> {
        let info = inverted_index_info(target.table(), spec)?;
        let pruner = InvertedIndexPruner::create_scored(
            target.fuse_table().get_operator(),
            target.ctx.cache_policy(),
            &info,
            candidates,
        )
        .await?;
        Ok(TextRetriever { pruner })
    }
}

#[async_trait::async_trait]
impl Retriever for TextRetriever {
    fn order(&self) -> ScoreOrder {
        ScoreOrder::HigherFirst
    }

    async fn search_block(&self, block: &BlockMeta) -> Result<Vec<BlockHit>> {
        let matches = self
            .pruner
            .should_keep(
                &block.location.0,
                block.inverted_index_metas.as_deref(),
                block.row_count,
            )
            .await?;
        let Some(matches) = matches else {
            return Ok(vec![]);
        };
        let scores = matches
            .scores
            .ok_or_else(|| ErrorCode::Internal("scored inverted index search without scores"))?;
        Ok(matches
            .rows
            .into_iter()
            .zip(scores)
            .map(|(row, score)| BlockHit {
                row,
                score: score.0,
            })
            .collect())
    }
}

/// The inverted index search of `spec`, built as the binder builds it for
/// `match(columns, query, options)` with `score()`.
fn inverted_index_info(table: &dyn Table, spec: &TextSpec) -> Result<InvertedIndexInfo> {
    let table_schema = table.schema();
    let mut query_fields = Vec::with_capacity(spec.columns.len());
    let mut column_ids = Vec::with_capacity(spec.columns.len());
    for column in &spec.columns {
        let (name, boost) = parse_boosted_column(column)?;
        column_ids.push(table_schema.column_id_of(name).map_err(|_| {
            ErrorCode::UnknownColumn(format!("table `{}` has no column `{name}`", table.name()))
        })?);
        query_fields.push((name.to_string(), boost.map(F32::from)));
    }

    let index = table
        .get_table_info()
        .meta
        .indexes
        .values()
        .find(|index| {
            index.index_type == TableIndexType::Inverted
                && column_ids.iter().all(|id| index.column_ids.contains(id))
        })
        .ok_or_else(|| {
            let names = query_fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>();
            ErrorCode::BadArguments(format!(
                "columns {} don't have inverted index",
                names.join(", ")
            ))
        })?;
    let index_fields = index
        .column_ids
        .iter()
        .map(|id| Ok(DataField::from(table_schema.field_of_column_id(*id)?)))
        .collect::<Result<Vec<_>>>()?;

    // `None` like `match` without options; the defaults search the same.
    let and = spec.operator == TextOperator::And;
    let inverted_index_option =
        (and || spec.lenient || spec.fuzziness.is_some()).then_some(InvertedIndexOption {
            fuzziness: spec.fuzziness,
            operator: and,
            lenient: spec.lenient,
        });

    Ok(InvertedIndexInfo {
        index_name: index.name.clone(),
        index_version: index.version.clone(),
        index_options: index.options.clone(),
        index_schema: DataSchema::new(index_fields),
        query_fields,
        query_text: spec.query.clone(),
        has_score: true,
        inverted_index_option,
    })
}

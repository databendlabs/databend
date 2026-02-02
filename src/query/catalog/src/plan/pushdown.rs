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
use std::collections::HashSet;
use std::fmt::Debug;

use databend_common_ast::ast::SampleConfig;
use databend_common_expression::ColumnId;
use databend_common_expression::DataSchema;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_storages_common_table_meta::table::ChangeType;
use jsonb::keypath::OwnedKeyPaths;

use super::AggIndexInfo;
use crate::plan::Projection;

/// Information of Virtual Columns.
///
/// Generated from the source column by the paths.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VirtualColumnInfo {
    /// The source column ids of virtual columns.
    /// If the virtual columns are not generated,
    /// we can read data from source column to generate them.
    pub source_column_ids: HashSet<u32>,
    /// The virtual column fields info.
    pub virtual_column_fields: Vec<VirtualColumnField>,
}

/// The virtual column field info.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VirtualColumnField {
    /// The source column id.
    pub source_column_id: u32,
    /// The source column name.
    pub source_name: String,
    /// The virtual column id
    pub column_id: u32,
    /// The virtual column name.
    pub name: String,
    /// Paths to generate virtual column from source column.
    pub key_paths: OwnedKeyPaths,
    /// optional cast function name, used to cast value to other type.
    pub cast_func_name: Option<String>,
    /// Virtual column data type.
    pub data_type: Box<TableDataType>,
}

/// Information about prewhere optimization.
///
/// Prewhere steps:
///
/// 1. Read columns by `prewhere_columns`.
/// 2. Filter data by `filter`.
/// 3. Read columns by `remain_columns`.
/// 4. If virtual columns are required, generate them from the source columns.
/// 5. Combine columns from step 1 and step 4, and prune columns to be `output_columns`.
///
/// **NOTE: the [`Projection`] is to be applied for the [`TableSchema`] of the data source.**
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PrewhereInfo {
    /// columns to be output by prewhere scan
    /// After building [`crate::plan::DataSourcePlan`],
    /// we can get the output schema after projection by `output_columns` from the plan directly.
    pub output_columns: Projection,
    /// columns of prewhere reading stage.
    pub prewhere_columns: Projection,
    /// columns of remain reading stage.
    pub remain_columns: Projection,
    /// filter for prewhere
    /// Assumption: expression's data type must be `DataType::Boolean`.
    pub filter: RemoteExpr<String>,
    /// Optional prewhere virtual column ids
    pub virtual_column_ids: Option<Vec<u32>>,
}

/// Inverted index option for additional search functions configuration.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InvertedIndexOption {
    /// Fuzzy query match terms within Levenshtein distance
    /// https://en.wikipedia.org/wiki/Levenshtein_distance
    /// For example: if fuzziness is 1, and query text if `fox`,
    /// the term `box` will be matched.
    pub fuzziness: Option<u8>,
    /// Operator: true is AND, false is OR, default is OR.
    /// For example: query text `happy tax payer` is equals to `happy OR tax OR payer`,
    /// but if operator is true, it will equals to `happy AND tax AND payer`.
    pub operator: bool,
    /// Parse a query leniently, ignore invalid query, default is false.
    pub lenient: bool,
}

/// Information about inverted index.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InvertedIndexInfo {
    /// The index name.
    pub index_name: String,
    /// The index version.
    pub index_version: String,
    /// The index options: tokenizer, filters, etc.
    pub index_options: BTreeMap<String, String>,
    /// The index schema.
    pub index_schema: DataSchema,
    /// The query field names and optional boost value,
    /// if boost is set, the score for the field is multiplied by the boost value.
    /// For example, if set `title^5.0, description^2.0`,
    /// it means that the score for `title` field is multiplied by 5.0,
    /// and the score for `description` field is multiplied by 2.0.
    pub query_fields: Vec<(String, Option<F32>)>,
    /// The search query text with query syntax.
    pub query_text: String,
    /// Whether search with score function.
    pub has_score: bool,
    /// Optional search configuration option, like fuzziness, lenient, ..
    pub inverted_index_option: Option<InvertedIndexOption>,
}

/// Information about vector index.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct VectorIndexInfo {
    /// The index name.
    pub index_name: String,
    /// The index version.
    pub index_version: String,
    /// The index options: m, ef_construct, ..
    pub index_options: BTreeMap<String, String>,
    /// The column id of vector column.
    pub column_id: ColumnId,
    /// The distance function name: l1_distance, l2_distance, cosine_distance, ..
    pub func_name: String,
    /// The query vector value.
    pub query_values: Vec<F32>,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct PushDownInfo {
    /// Optional column indices to use as a projection.
    /// It represents the columns to be read from the source.
    pub projection: Option<Projection>,
    /// Optional column indices as output by the scan, only used when having virtual columns.
    /// The difference with `projection` is the removal of the source columns
    /// which were only used to generate virtual columns.
    pub output_columns: Option<Projection>,
    /// Optional filter and reverse filter expression plan
    /// Assumption: expression's data type must be `DataType::Boolean`.
    pub filters: Option<Filters>,
    pub is_deterministic: bool,
    /// Optional prewhere information used for prewhere optimization.
    pub prewhere: Option<PrewhereInfo>,
    /// Optional limit to skip read.
    pub limit: Option<usize>,
    /// Optional order_by expression plan, asc, null_first.
    pub order_by: Vec<(RemoteExpr<String>, bool, bool)>,
    /// Optional virtual columns
    pub virtual_column: Option<VirtualColumnInfo>,
    /// If lazy materialization is enabled in this query.
    pub lazy_materialization: bool,
    /// Aggregating index information.
    pub agg_index: Option<AggIndexInfo>,
    /// Identifies the type of data change we are looking for
    pub change_type: Option<ChangeType>,
    /// Optional inverted index
    pub inverted_index: Option<InvertedIndexInfo>,
    /// Optional vector index
    pub vector_index: Option<VectorIndexInfo>,
    /// Used by table sample
    pub sample: Option<SampleConfig>,
}

impl PushDownInfo {
    pub fn filter_only_use_index(&self) -> bool {
        let allow_search = self.inverted_index.is_some();
        if !allow_search {
            return false;
        }
        if let Some(filters) = &self.filters {
            remote_expr_only_uses_index_columns(&filters.filter)
        } else {
            false
        }
    }
}

fn remote_expr_only_uses_index_columns(expr: &RemoteExpr<String>) -> bool {
    // @TODO support other inverted index filter expr, like score() > 1.0
    if let RemoteExpr::ColumnRef { id, .. } = expr {
        if id == SEARCH_MATCHED_COL_NAME {
            return true;
        }
    }
    false
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Filters {
    pub filter: RemoteExpr<String>,
    pub inverted_filter: RemoteExpr<String>,
}

/// TopK is a wrapper for topk push down items.
/// We only take the first column in order_by as the topk column.
#[derive(Debug, Clone)]
pub struct TopK {
    pub limit: usize,
    /// Record the leaf field of the topk column.
    /// - The `name` of `field` will be used to track column in the read block.
    /// - The `column_id` of `field` will be used to retrieve column stats from block meta
    ///   (only used for fuse engine, for parquet table, we will use `leaf_id`).
    pub field: TableField,
    pub asc: bool,
    /// The index in `table_schema.leaf_fields()`.
    /// It's only used for external parquet files reading.
    pub leaf_id: usize,
}

impl TopK {
    fn support_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Number(_)
                | DataType::Date
                | DataType::Timestamp
                | DataType::String
                | DataType::Decimal(_)
        )
    }
}

pub const TOPK_PUSHDOWN_THRESHOLD: usize = 1000;

impl PushDownInfo {
    pub fn top_k(&self, schema: &TableSchema) -> Option<TopK> {
        if !self.order_by.is_empty() && self.limit.is_some() {
            let order = &self.order_by[0];
            let limit = self.limit.unwrap();

            if limit > TOPK_PUSHDOWN_THRESHOLD {
                return None;
            }

            if let RemoteExpr::<String>::ColumnRef { id, data_type, .. } = &order.0 {
                // TODO: support sub column of nested type.
                if !TopK::support_type(data_type) {
                    return None;
                }
                let leaf_fields = schema.leaf_fields();
                leaf_fields
                    .iter()
                    .enumerate()
                    .find(|&(_, p)| p.name() == id)
                    .map(|(leaf_id, f)| TopK {
                        limit: self.limit.unwrap(),
                        field: f.clone(),
                        asc: order.1,
                        leaf_id,
                    })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn prewhere_of_push_downs(push_downs: Option<&PushDownInfo>) -> Option<PrewhereInfo> {
        if let Some(PushDownInfo { prewhere, .. }) = push_downs {
            prewhere.clone()
        } else {
            None
        }
    }

    pub fn projection_of_push_downs(
        schema: &TableSchema,
        push_downs: Option<&PushDownInfo>,
    ) -> Projection {
        if let Some(PushDownInfo {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            let indices = (0..schema.fields().len()).collect::<Vec<usize>>();
            Projection::Columns(indices)
        }
    }

    pub fn virtual_columns_of_push_downs(
        push_downs: &Option<PushDownInfo>,
    ) -> Option<VirtualColumnInfo> {
        if let Some(PushDownInfo { virtual_column, .. }) = push_downs {
            virtual_column.clone()
        } else {
            None
        }
    }
}

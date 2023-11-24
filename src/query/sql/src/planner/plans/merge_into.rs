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
use std::collections::HashSet;

use common_ast::ast::TableAlias;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_meta_types::MetaId;

use crate::optimizer::SExpr;
use crate::BindContext;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

// for unmatched clause, we need to calculate the
#[derive(Clone, Debug)]
pub struct UnmatchedEvaluator {
    pub source_schema: DataSchemaRef,
    pub condition: Option<ScalarExpr>,
    pub values: Vec<ScalarExpr>,
}

#[derive(Clone, Debug)]
pub struct MatchedEvaluator {
    pub condition: Option<ScalarExpr>,
    // table_schema.idx -> update_expression
    // Some => update
    // None => delete
    pub update: Option<HashMap<FieldIndex, ScalarExpr>>,
}

#[derive(Clone)]
pub struct MergeInto {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub target_alias: Option<TableAlias>,
    pub table_id: MetaId,
    pub input: Box<SExpr>,
    pub bind_context: Box<BindContext>,
    pub columns_set: Box<HashSet<IndexType>>,
    pub meta_data: MetadataRef,
    pub matched_evaluators: Vec<MatchedEvaluator>,
    pub unmatched_evaluators: Vec<UnmatchedEvaluator>,
    pub target_table_idx: usize,
    pub field_index_map: HashMap<FieldIndex, String>,
}

impl std::fmt::Debug for MergeInto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Merge Into")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("table_id", &self.table_id)
            .field("join", &self.input)
            .field("matched", &self.matched_evaluators)
            .field("unmateched", &self.unmatched_evaluators)
            .finish()
    }
}

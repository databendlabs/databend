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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_expression::Symbol;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;

use super::ScalarExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::binder::MutationType;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics as OpStatistics;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, Default)]
pub struct MutationSource {
    pub schema: TableSchema,
    pub columns: ColumnSet,
    pub table_index: IndexType,
    pub mutation_type: MutationType,
    pub secure_predicates: Vec<ScalarExpr>,
    pub user_predicates: Vec<ScalarExpr>,
    pub predicate_column_index: Option<Symbol>,
    pub read_partition_columns: ColumnSet,
    /// True when this mutation target is subject to a Row Access Policy.
    pub has_row_access_policy: bool,
}

impl PartialEq for MutationSource {
    fn eq(&self, other: &Self) -> bool {
        self.table_index == other.table_index && self.columns == other.columns
    }
}

impl Eq for MutationSource {}

impl std::hash::Hash for MutationSource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_index.hash(state);
        for column in self.columns.iter() {
            column.hash(state);
        }
    }
}

impl Operator for MutationSource {
    fn rel_op(&self) -> RelOp {
        RelOp::MutationSource
    }

    fn arity(&self) -> usize {
        0
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(
            self.secure_predicates
                .iter()
                .chain(self.user_predicates.iter()),
        )
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns: Default::default(),
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 0.0,
            statistics: OpStatistics {
                precise_cardinality: None,
                column_stats: Default::default(),
            },
        }))
    }

    // Won't be invoked at all, since `PhysicalScan` is leaf node
    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "Cannot compute required property for children of MutationSource".to_string(),
        ))
    }
}

impl MutationSource {
    pub fn all_predicates(&self) -> impl Iterator<Item = &ScalarExpr> + '_ {
        self.secure_predicates
            .iter()
            .chain(self.user_predicates.iter())
    }

    pub fn has_predicates(&self) -> bool {
        !self.secure_predicates.is_empty() || !self.user_predicates.is_empty()
    }

    pub fn refresh_read_partition_columns(&mut self) {
        self.read_partition_columns = self
            .all_predicates()
            .flat_map(|predicate| predicate.used_columns())
            .collect();
    }

    /// Return all predicates (secure + user) as an owned Vec.
    /// Used to populate `Mutation::direct_filter` for push-down.
    pub fn all_predicates_cloned(&self) -> Vec<ScalarExpr> {
        self.all_predicates().cloned().collect()
    }

    pub fn ensure_mutation_predicate_column_if_needed(
        &mut self,
        metadata: &MetadataRef,
    ) -> Option<Symbol> {
        if self.mutation_type != MutationType::Update || !self.has_predicates() {
            return None;
        }

        Some(*self.predicate_column_index.get_or_insert_with(|| {
            metadata
                .write()
                .add_derived_column(PREDICATE_COLUMN_NAME.to_string(), DataType::Boolean)
        }))
    }
}

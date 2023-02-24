// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;

pub struct RulePushDownFilterScan {
    id: RuleID,
    pattern: SExpr,
    metadata: MetadataRef,
}

impl RulePushDownFilterScan {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterScan,
            // Filter
            //  \
            //   LogicalGet
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Scan,
                    }
                    .into(),
                ),
            ),
            metadata,
        }
    }

    fn find_push_down_predicates(&self, predicates: &[ScalarExpr]) -> Result<Vec<ScalarExpr>> {
        let mut filtered_predicates = vec![];
        for predicate in predicates {
            let used_columns = predicate.used_columns();
            let metadata = self.metadata.read();
            let column_entries = metadata.columns();
            let mut contain_derived_column = false;
            for column_entry in column_entries {
                match column_entry {
                    ColumnEntry::BaseTableColumn(_) => {}
                    ColumnEntry::DerivedColumn(column) => {
                        // Don't push down predicate that contains derived column
                        // Because storage can't know such columns.
                        if used_columns.contains(&column.column_index) {
                            contain_derived_column = true;
                            break;
                        }
                    }
                }
            }
            if !contain_derived_column {
                filtered_predicates.push(predicate.clone());
            }
        }

        Ok(filtered_predicates)
    }
}

impl Rule for RulePushDownFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut get: Scan = s_expr.child(0)?.plan().clone().try_into()?;

        if get.push_down_predicates.is_some() {
            return Ok(());
        }

        get.push_down_predicates = Some(self.find_push_down_predicates(&filter.predicates)?);

        let result = SExpr::create_unary(filter.into(), SExpr::create_leaf(get.into()));
        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

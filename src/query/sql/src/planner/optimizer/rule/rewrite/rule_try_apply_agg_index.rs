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

use databend_common_exception::Result;

use super::agg_index;
use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    metadata: MetadataRef,

    matchers: Vec<Matcher>,
}

impl RuleTryApplyAggIndex {
    fn sorted_matchers() -> Vec<Matcher> {
        vec![
            // Expression
            //     |
            //    Sort
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            },
            // Expression
            //     |
            //    Sort
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Filter,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                }],
            },
            // Expression
            //     |
            //    Sort
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::EvalScalar,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::Scan,
                                    children: vec![],
                                }],
                            }],
                        }],
                    }],
                }],
            },
            // Expression
            //     |
            //    Sort
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::EvalScalar,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::Filter,
                                    children: vec![Matcher::MatchOp {
                                        op_type: RelOp::Scan,
                                        children: vec![],
                                    }],
                                }],
                            }],
                        }],
                    }],
                }],
            },
        ]
    }

    fn normal_matchers() -> Vec<Matcher> {
        vec![
            // Expression
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            },
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            },
            // Expression
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::EvalScalar,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Scan,
                                children: vec![],
                            }],
                        }],
                    }],
                }],
            },
            // Expression
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::EvalScalar,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Filter,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::Scan,
                                    children: vec![],
                                }],
                            }],
                        }],
                    }],
                }],
            },
        ]
    }

    fn matchers() -> Vec<Matcher> {
        let mut patterns = Self::normal_matchers();
        patterns.extend(Self::sorted_matchers());
        patterns
    }

    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            metadata,
            matchers: Self::matchers(),
        }
    }
}

impl Rule for RuleTryApplyAggIndex {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let (table_index, table_name) = self.get_table(s_expr);
        let metadata = self.metadata.read();
        let index_plans = metadata.get_agg_indexes(&table_name);
        if index_plans.is_none() {
            // No enterprise license or no index.
            return Ok(());
        }
        let index_plans = index_plans.unwrap();
        if index_plans.is_empty() {
            // No enterprise license or no index.
            return Ok(());
        }

        let base_columns = metadata.columns_by_table_index(table_index);
        let table_name = metadata.table(table_index).name();

        if let Some(mut result) =
            agg_index::try_rewrite(table_index, table_name, &base_columns, s_expr, index_plans)?
        {
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl RuleTryApplyAggIndex {
    fn get_table(&self, s_expr: &SExpr) -> (IndexType, String) {
        match s_expr.plan() {
            RelOperator::Scan(scan) => {
                let metadata = self.metadata.read();
                let table = metadata.table(scan.table_index);
                (
                    scan.table_index,
                    format!("{}.{}.{}", table.catalog(), table.database(), table.name()),
                )
            }
            _ => self.get_table(s_expr.child(0).unwrap()),
        }
    }
}

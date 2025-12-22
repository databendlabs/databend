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
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::ConstantTableScan;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;

pub struct RulePushDownLimit {
    id: RuleID,
    metadata: MetadataRef,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimit {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownLimit,
            metadata,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RulePushDownLimit {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(limit_val) = limit.limit
            && limit_val == 0
        {
            let output_columns = limit
                .derive_relational_prop(&RelExpr::with_s_expr(s_expr))?
                .output_columns
                .clone();
            let metadata = self.metadata.read();
            let mut fields = Vec::with_capacity(output_columns.len());
            for col in output_columns.iter() {
                fields.push(DataField::new(
                    &col.to_string(),
                    metadata.column(*col).data_type(),
                ));
            }
            let empty_scan =
                ConstantTableScan::new_empty_scan(DataSchemaRefExt::create(fields), output_columns);
            let result = SExpr::create_leaf(Arc::new(RelOperator::ConstantTableScan(empty_scan)));
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

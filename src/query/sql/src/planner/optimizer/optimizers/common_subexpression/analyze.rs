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
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::common_subexpression::rewrite::SExprReplacement;
use crate::optimizer::optimizers::common_subexpression::table_signature::collect_table_signatures;
use crate::planner::metadata::Metadata;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;
pub fn analyze_common_subexpression(
    s_expr: &SExpr,
    metadata: &Metadata,
) -> Result<(Vec<SExprReplacement>, Vec<SExpr>)> {
    let signature_to_exprs = collect_table_signatures(s_expr, metadata);
    let mut replacements = vec![];
    let mut materialized_ctes = vec![];
    for exprs in signature_to_exprs.values() {
        process_candidate_expressions(exprs, &mut replacements, &mut materialized_ctes, metadata)?;
    }
    Ok((replacements, materialized_ctes))
}

fn process_candidate_expressions(
    candidates: &[(Vec<usize>, SExpr)],
    replacements: &mut Vec<SExprReplacement>,
    materialized_ctes: &mut Vec<SExpr>,
    _metadata: &Metadata,
) -> Result<()> {
    if candidates.len() < 2 {
        return Ok(());
    }

    let cte_def = &candidates[0].1;
    let cte_def_columns = cte_def.derive_relational_prop()?.output_columns.clone();
    let cte_name = format!("cte_cse_{}", materialized_ctes.len());

    let cte_plan = MaterializedCTE::new(cte_name.clone(), None, None);
    let cte_expr = SExpr::create_unary(
        Arc::new(RelOperator::MaterializedCTE(cte_plan)),
        Arc::new(cte_def.clone()),
    );
    materialized_ctes.push(cte_expr);

    for (path, expr) in candidates {
        let cte_ref_columns = expr.derive_relational_prop()?.output_columns.clone();
        let column_mapping = cte_ref_columns
            .iter()
            .copied()
            .zip(cte_def_columns.iter().copied())
            .collect::<HashMap<_, _>>();
        let cte_ref = MaterializedCTERef {
            cte_name: cte_name.clone(),
            output_columns: cte_ref_columns.iter().copied().collect(),
            def: expr.clone(),
            column_mapping,
        };
        let cte_ref_expr = Arc::new(SExpr::create_leaf(Arc::new(
            RelOperator::MaterializedCTERef(cte_ref),
        )));
        replacements.push(SExprReplacement {
            path: path.clone(),
            new_expr: cte_ref_expr.clone(),
        });
    }
    Ok(())
}

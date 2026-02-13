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

use super::rewrite::SExprReplacement;
use super::signature::compute_s_expr_signatures;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;

struct CandidateGroup<'a> {
    signature: &'a super::signature::SExprSignature,
    candidates: &'a [(Vec<usize>, SExpr)],
    max_complexity: usize,
    min_path: Vec<usize>,
}

pub fn analyze_common_subexpression(
    s_expr: &SExpr,
    metadata: &mut Metadata,
) -> Result<(Vec<SExprReplacement>, Vec<SExpr>)> {
    // Skip CSE optimization if the expression contains recursive CTE
    if contains_recursive_cte(s_expr) {
        return Ok((vec![], vec![]));
    }

    let signature_to_exprs = compute_s_expr_signatures(s_expr, metadata);
    let mut replacements = vec![];
    let mut materialized_ctes = vec![];
    let mut reused_paths = vec![];

    let mut candidate_groups = signature_to_exprs
        .iter()
        // Only consider single-table CSE candidates.
        .filter(|(signature, _)| signature.tables.len() == 1)
        .map(|(signature, candidates)| CandidateGroup {
            signature,
            candidates: candidates.as_slice(),
            max_complexity: max_candidate_complexity(candidates),
            min_path: min_candidate_path(candidates),
        })
        .collect::<Vec<_>>();

    // Always prefer reusing more complex expressions first.
    candidate_groups.sort_by(|a, b| {
        b.max_complexity
            .cmp(&a.max_complexity)
            .then_with(|| a.min_path.cmp(&b.min_path))
            .then_with(|| a.signature.cmp(b.signature))
    });

    for group in candidate_groups {
        let paths = process_candidate_expressions(
            group.candidates,
            metadata,
            &mut replacements,
            &mut materialized_ctes,
            &reused_paths,
        )?;
        reused_paths.extend(paths);
    }
    Ok((replacements, materialized_ctes))
}

fn process_candidate_expressions(
    candidates: &[(Vec<usize>, SExpr)],
    metadata: &mut Metadata,
    replacements: &mut Vec<SExprReplacement>,
    materialized_ctes: &mut Vec<SExpr>,
    skip_paths: &[Vec<usize>],
) -> Result<Vec<Vec<usize>>> {
    let mut candidates = candidates
        .iter()
        .filter(|(path, _)| !is_path_covered(path, skip_paths))
        .map(|candidate| (expr_complexity(&candidate.1), candidate))
        .collect::<Vec<_>>();

    // Prefer complex candidates and suppress nested simpler candidates.
    candidates.sort_by(|(a_complexity, a), (b_complexity, b)| {
        b_complexity.cmp(a_complexity).then_with(|| a.0.cmp(&b.0))
    });

    let mut selected_paths = vec![];
    let mut selected_candidates = vec![];
    for (_, candidate) in candidates {
        if is_path_covered(&candidate.0, &selected_paths) {
            continue;
        }
        selected_paths.push(candidate.0.clone());
        selected_candidates.push(candidate);
    }

    if selected_candidates.len() < 2 {
        return Ok(vec![]);
    }

    let mut cte_def = selected_candidates[0].1.clone();
    if let RelOperator::Scan(scan) = cte_def.plan.as_ref() {
        let mut scan = scan.clone();
        scan.scan_id = metadata.next_scan_id();
        cte_def = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
    }
    let cte_def = Arc::new(cte_def);

    let cte_def_columns = cte_def.derive_relational_prop()?.output_columns.clone();
    let cte_name = format!("cte_cse_{}", materialized_ctes.len());

    let cte_plan = MaterializedCTE::new(cte_name.clone(), None);
    let cte_expr = SExpr::create_unary(
        Arc::new(RelOperator::MaterializedCTE(cte_plan)),
        cte_def.clone(),
    );
    materialized_ctes.push(cte_expr);

    let mut processed_paths = Vec::with_capacity(selected_candidates.len());
    for (path, expr) in selected_candidates {
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
        processed_paths.push(path.clone());
    }
    Ok(processed_paths)
}

fn is_path_covered(path: &[usize], prefixes: &[Vec<usize>]) -> bool {
    prefixes
        .iter()
        .any(|prefix| path.starts_with(prefix.as_slice()))
}

fn expr_complexity(expr: &SExpr) -> usize {
    1 + expr.children().map(expr_complexity).sum::<usize>()
}

fn max_candidate_complexity(candidates: &[(Vec<usize>, SExpr)]) -> usize {
    candidates
        .iter()
        .map(|(_, expr)| expr_complexity(expr))
        .max()
        .unwrap_or_default()
}

fn min_candidate_path(candidates: &[(Vec<usize>, SExpr)]) -> Vec<usize> {
    candidates
        .iter()
        .map(|(path, _)| path.clone())
        .min()
        .unwrap_or_default()
}

fn contains_recursive_cte(expr: &SExpr) -> bool {
    if matches!(expr.plan(), RelOperator::RecursiveCteScan(_)) {
        return true;
    }

    expr.children().any(contains_recursive_cte)
}

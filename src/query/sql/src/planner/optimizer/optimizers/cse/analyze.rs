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
use super::signature::SExprSignature;
use super::signature::compute_s_expr_signatures;
use crate::ColumnEntry;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

struct CandidateGroup<'a> {
    signature: &'a SExprSignature,
    candidates: &'a [(Vec<usize>, SExpr)],
    max_complexity: usize,
    min_path: Vec<usize>,
}

#[derive(Clone, Debug)]
struct RewritableCandidate {
    path: Vec<usize>,
    expr: SExpr,
    extracted_filters: Vec<ScalarExpr>,
    complexity: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CandidateRewriteKey {
    NoJoin,
    Join(JoinTreeSignature),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum JoinTreeSignature {
    Scan {
        table_id: u64,
    },
    Join {
        join_type: crate::plans::JoinType,
        equi_conditions: Vec<JoinEquiConditionSignature>,
        non_equi_conditions: Vec<ScalarExpr>,
        left: Box<JoinTreeSignature>,
        right: Box<JoinTreeSignature>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct JoinEquiConditionSignature {
    left: ScalarExpr,
    right: ScalarExpr,
    is_null_equal: bool,
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
            group.signature,
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
    signature: &SExprSignature,
    candidates: &[(Vec<usize>, SExpr)],
    metadata: &mut Metadata,
    replacements: &mut Vec<SExprReplacement>,
    materialized_ctes: &mut Vec<SExpr>,
    skip_paths: &[Vec<usize>],
) -> Result<Vec<Vec<usize>>> {
    let mut candidates = candidates
        .iter()
        .filter(|(path, _)| !is_path_covered(path, skip_paths))
        .map(|(path, expr)| {
            let (expr, extracted_filters) = if signature.has_aggregate {
                (expr.clone(), vec![])
            } else {
                extract_filters_for_cse(expr)?
            };
            let complexity = expr_complexity(&expr);
            Ok(RewritableCandidate {
                path: path.clone(),
                expr,
                extracted_filters,
                complexity,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Prefer complex candidates and suppress nested simpler candidates.
    candidates.sort_by(|a, b| {
        b.complexity
            .cmp(&a.complexity)
            .then_with(|| a.path.cmp(&b.path))
    });

    let mut selected_paths = vec![];
    let mut selected_candidates = vec![];
    for candidate in candidates {
        if is_path_covered(&candidate.path, &selected_paths) {
            continue;
        }
        selected_paths.push(candidate.path.clone());
        selected_candidates.push(candidate);
    }

    let mut rewrite_groups: Vec<(CandidateRewriteKey, Vec<RewritableCandidate>)> = vec![];
    for candidate in selected_candidates {
        let Some(key) = candidate_rewrite_key(&candidate.expr, metadata) else {
            continue;
        };
        if let Some((_, group)) = rewrite_groups
            .iter_mut()
            .find(|(group_key, _)| *group_key == key)
        {
            group.push(candidate);
        } else {
            rewrite_groups.push((key, vec![candidate]));
        }
    }

    let mut processed_paths = vec![];
    for (_, group) in rewrite_groups {
        let paths = rewrite_candidate_group(group, metadata, replacements, materialized_ctes)?;
        processed_paths.extend(paths);
    }
    Ok(processed_paths)
}

fn rewrite_candidate_group(
    candidates: Vec<RewritableCandidate>,
    metadata: &mut Metadata,
    replacements: &mut Vec<SExprReplacement>,
    materialized_ctes: &mut Vec<SExpr>,
) -> Result<Vec<Vec<usize>>> {
    if candidates.len() < 2 {
        return Ok(vec![]);
    }

    let mut cte_def = candidates[0].expr.clone();
    if let RelOperator::Scan(scan) = cte_def.plan.as_ref() {
        let mut scan = scan.clone();
        scan.scan_id = metadata.next_scan_id();
        cte_def = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
    }
    let cte_def = Arc::new(cte_def);

    let cte_def_columns = cte_def.derive_relational_prop()?.output_columns.clone();
    let mut compatible_candidates = vec![];
    for candidate in candidates {
        let cte_ref_columns = candidate
            .expr
            .derive_relational_prop()?
            .output_columns
            .clone();
        if cte_ref_columns.len() != cte_def_columns.len() {
            continue;
        }
        compatible_candidates.push((candidate, cte_ref_columns));
    }

    if compatible_candidates.len() < 2 {
        return Ok(vec![]);
    }

    let cte_name = format!("cte_cse_{}", materialized_ctes.len());
    let cte_plan = MaterializedCTE::new(cte_name.clone(), None);
    let cte_expr = SExpr::create_unary(
        Arc::new(RelOperator::MaterializedCTE(cte_plan)),
        cte_def.clone(),
    );
    materialized_ctes.push(cte_expr);

    let mut processed_paths = Vec::with_capacity(compatible_candidates.len());
    for (candidate, cte_ref_columns) in compatible_candidates {
        let column_mapping = cte_ref_columns
            .iter()
            .copied()
            .zip(cte_def_columns.iter().copied())
            .collect::<HashMap<_, _>>();
        let cte_ref = MaterializedCTERef {
            cte_name: cte_name.clone(),
            output_columns: cte_ref_columns.iter().copied().collect(),
            def: candidate.expr.clone(),
            column_mapping,
        };
        let cte_ref_expr = Arc::new(SExpr::create_leaf(Arc::new(
            RelOperator::MaterializedCTERef(cte_ref),
        )));

        let new_expr = if candidate.extracted_filters.is_empty() {
            cte_ref_expr
        } else {
            Arc::new(SExpr::create_unary(
                Arc::new(RelOperator::Filter(Filter {
                    predicates: candidate.extracted_filters.clone(),
                })),
                cte_ref_expr,
            ))
        };

        replacements.push(SExprReplacement {
            path: candidate.path.clone(),
            new_expr,
        });
        processed_paths.push(candidate.path.clone());
    }

    Ok(processed_paths)
}

fn candidate_rewrite_key(expr: &SExpr, metadata: &Metadata) -> Option<CandidateRewriteKey> {
    if !contains_join(expr) {
        return Some(CandidateRewriteKey::NoJoin);
    }
    Some(CandidateRewriteKey::Join(build_join_tree_signature(
        expr, metadata,
    )?))
}

fn contains_join(expr: &SExpr) -> bool {
    if matches!(expr.plan(), RelOperator::Join(_)) {
        return true;
    }
    expr.children().any(contains_join)
}

fn build_join_tree_signature(expr: &SExpr, metadata: &Metadata) -> Option<JoinTreeSignature> {
    match expr.plan() {
        RelOperator::Scan(scan) => {
            let table_id = metadata.table(scan.table_index).table().get_id();
            Some(JoinTreeSignature::Scan { table_id })
        }
        RelOperator::Join(join) => {
            if join.from_correlated_subquery || join.is_lateral {
                return None;
            }
            if !matches!(
                join.join_type,
                crate::plans::JoinType::Inner | crate::plans::JoinType::Cross
            ) {
                return None;
            }

            let left = build_join_tree_signature(expr.child(0).ok()?, metadata)?;
            let right = build_join_tree_signature(expr.child(1).ok()?, metadata)?;
            let equi_conditions = join
                .equi_conditions
                .iter()
                .map(|cond| {
                    Some(JoinEquiConditionSignature {
                        left: normalize_scalar_expr(&cond.left, metadata)?,
                        right: normalize_scalar_expr(&cond.right, metadata)?,
                        is_null_equal: cond.is_null_equal,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            let non_equi_conditions = join
                .non_equi_conditions
                .iter()
                .map(|condition| normalize_scalar_expr(condition, metadata))
                .collect::<Option<Vec<_>>>()?;

            Some(JoinTreeSignature::Join {
                join_type: join.join_type,
                equi_conditions,
                non_equi_conditions,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        _ => None,
    }
}

fn normalize_scalar_expr(expr: &ScalarExpr, metadata: &Metadata) -> Option<ScalarExpr> {
    let mut normalized = expr.clone();
    let mut normalizer = PhysicalColumnNormalizer {
        metadata,
        valid: true,
    };
    normalizer.visit(&mut normalized).ok()?;
    normalizer.valid.then_some(normalized)
}

struct PhysicalColumnNormalizer<'a> {
    metadata: &'a Metadata,
    valid: bool,
}

impl VisitorMut<'_> for PhysicalColumnNormalizer<'_> {
    fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
        let Some((table_id, key)) = physical_column_key(self.metadata, col.column.index) else {
            self.valid = false;
            return Ok(());
        };

        col.column.database_name = None;
        col.column.table_name = None;
        col.column.column_position = None;
        col.column.table_index = Some(table_id as usize);
        col.column.column_name = key;
        col.column.index = 0;
        col.column.virtual_expr = None;
        Ok(())
    }
}

fn physical_column_key(metadata: &Metadata, index: usize) -> Option<(u64, String)> {
    match metadata.column(index) {
        ColumnEntry::BaseTableColumn(base_col) => {
            let table_id = metadata.table(base_col.table_index).table().get_id();
            Some((
                table_id,
                format!(
                    "base:table_id={table_id}:column_id={:?}:column_pos={:?}:column_name={}:path={:?}",
                    base_col.column_id,
                    base_col.column_position,
                    base_col.column_name,
                    base_col.path_indices
                ),
            ))
        }
        ColumnEntry::InternalColumn(internal_col) => {
            let table_id = metadata.table(internal_col.table_index).table().get_id();
            Some((
                table_id,
                format!(
                    "internal:table_id={table_id}:column_name={}",
                    internal_col.internal_column.column_name
                ),
            ))
        }
        ColumnEntry::VirtualColumn(virtual_col) => {
            let table_id = metadata.table(virtual_col.table_index).table().get_id();
            Some((
                table_id,
                format!(
                    "virtual:table_id={table_id}:source_column_id={}:column_id={}:column_name={}",
                    virtual_col.source_column_id, virtual_col.column_id, virtual_col.column_name
                ),
            ))
        }
        ColumnEntry::DerivedColumn(_) => None,
    }
}

fn extract_filters_for_cse(expr: &SExpr) -> Result<(SExpr, Vec<ScalarExpr>)> {
    match expr.plan() {
        RelOperator::Filter(filter) => {
            let (child, mut child_filters) = extract_filters_for_cse(expr.child(0)?)?;
            let mut predicates = filter.predicates.clone();
            predicates.append(&mut child_filters);
            Ok((child, predicates))
        }
        RelOperator::Scan(scan) => {
            let mut predicates = vec![];
            if let Some(push_down_predicates) = &scan.push_down_predicates {
                predicates.extend(push_down_predicates.clone());
            }
            if let Some(prewhere) = &scan.prewhere {
                predicates.extend(prewhere.predicates.clone());
            }

            if predicates.is_empty() {
                return Ok((expr.clone(), vec![]));
            }

            let mut new_scan = scan.clone();
            let mut output_columns = new_scan.columns.clone();
            for predicate in predicates.iter() {
                output_columns.extend(predicate.used_columns());
            }
            new_scan.columns = output_columns;
            new_scan.push_down_predicates = None;
            new_scan.prewhere = None;

            Ok((
                SExpr::create_leaf(Arc::new(RelOperator::Scan(new_scan))),
                predicates,
            ))
        }
        _ => {
            let mut extracted_filters = vec![];
            let mut new_children = Vec::with_capacity(expr.children.len());
            let mut has_child_changed = false;
            for child in expr.children() {
                let (new_child, mut child_filters) = extract_filters_for_cse(child)?;
                if !new_child.eq(child) {
                    has_child_changed = true;
                }
                extracted_filters.append(&mut child_filters);
                new_children.push(Arc::new(new_child));
            }
            let new_expr = if has_child_changed {
                expr.replace_children(new_children)
            } else {
                expr.clone()
            };
            Ok((new_expr, extracted_filters))
        }
    }
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

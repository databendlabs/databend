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
use super::table_signature::collect_table_signatures;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;

pub fn analyze_common_subexpression(
    s_expr: &SExpr,
    metadata: &mut Metadata,
) -> Result<(Vec<SExprReplacement>, Vec<SExpr>)> {
    // Skip CSE optimization if the expression contains recursive CTE
    if contains_recursive_cte(s_expr) {
        return Ok((vec![], vec![]));
    }

    let signature_to_exprs = collect_table_signatures(s_expr, metadata);
    let mut expr_groups = signature_to_exprs.into_values().collect::<Vec<_>>();
    // Keep CSE materialization order deterministic by following the first
    // occurrence of each candidate group in the plan tree.
    expr_groups.sort_by(|lhs, rhs| lhs[0].0.cmp(&rhs[0].0));
    let mut replacements = vec![];
    let mut materialized_ctes = vec![];
    let mut selected_paths = vec![];
    for exprs in &expr_groups {
        process_candidate_expressions(
            exprs,
            metadata,
            &mut replacements,
            &mut materialized_ctes,
            &mut selected_paths,
        )?;
    }
    Ok((replacements, materialized_ctes))
}

fn process_candidate_expressions(
    candidates: &[(Vec<usize>, SExpr)],
    metadata: &mut Metadata,
    replacements: &mut Vec<SExprReplacement>,
    materialized_ctes: &mut Vec<SExpr>,
    selected_paths: &mut Vec<Vec<usize>>,
) -> Result<()> {
    let candidates = candidates
        .iter()
        .filter(|(path, _)| {
            !selected_paths
                .iter()
                .any(|selected| paths_overlap(path, selected))
        })
        .cloned()
        .collect::<Vec<_>>();
    if candidates.len() < 2 {
        return Ok(());
    }

    let cte_def = refresh_scan_ids(&candidates[0].1, metadata)?;
    let cte_def = Arc::new(cte_def);

    let cte_def_columns = cte_def.derive_relational_prop()?.output_columns.clone();
    let cte_name = format!("cte_cse_{}", materialized_ctes.len());

    let cte_plan = MaterializedCTE::new(cte_name.clone(), None);
    let cte_expr = SExpr::create_unary(
        Arc::new(RelOperator::MaterializedCTE(cte_plan)),
        cte_def.clone(),
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
            stat_info: None,
        };
        let cte_ref_expr = Arc::new(SExpr::create_leaf(Arc::new(
            RelOperator::MaterializedCTERef(cte_ref),
        )));
        replacements.push(SExprReplacement {
            path: path.clone(),
            new_expr: cte_ref_expr.clone(),
        });
        selected_paths.push(path);
    }
    Ok(())
}

#[recursive::recursive]
fn refresh_scan_ids(s_expr: &SExpr, metadata: &mut Metadata) -> Result<SExpr> {
    let new_children = s_expr
        .children()
        .map(|child| refresh_scan_ids(child, metadata))
        .collect::<Result<Vec<_>>>()?;

    let mut result = if new_children
        .iter()
        .zip(s_expr.children())
        .any(|(new, old)| !new.eq(old))
    {
        s_expr.replace_children(new_children.into_iter().map(Arc::new))
    } else {
        s_expr.clone()
    };

    if let RelOperator::Scan(scan) = result.plan.as_ref() {
        let mut scan = scan.clone();
        scan.scan_id = metadata.next_scan_id();
        result = result.replace_plan(Arc::new(RelOperator::Scan(scan)));
    }

    Ok(result)
}

fn paths_overlap(lhs: &[usize], rhs: &[usize]) -> bool {
    lhs.starts_with(rhs) || rhs.starts_with(lhs)
}

fn contains_recursive_cte(expr: &SExpr) -> bool {
    if matches!(expr.plan(), RelOperator::RecursiveCteScan(_)) {
        return true;
    }

    expr.children().any(contains_recursive_cte)
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use databend_common_catalog::table::Table;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::schema::CatalogInfo;
    use databend_common_meta_app::schema::DatabaseType;
    use databend_common_meta_app::schema::TableIdent;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::schema::TableMeta;

    use super::*;
    use crate::planner::metadata::Metadata;
    use crate::plans::Join;
    use crate::plans::JoinType;
    use crate::plans::RelOperator;
    use crate::plans::Scan;

    #[derive(Debug)]
    struct FakeTable {
        table_info: TableInfo,
    }

    #[async_trait::async_trait]
    impl Table for FakeTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_table_info(&self) -> &TableInfo {
            &self.table_info
        }

        fn support_column_projection(&self) -> bool {
            true
        }
    }

    fn fake_fuse_table(table_id: u64, table_name: &str) -> Arc<dyn Table> {
        Arc::new(FakeTable {
            table_info: TableInfo {
                ident: TableIdent::new(table_id, 0),
                desc: format!("'default'.'{table_name}'"),
                name: table_name.to_string(),
                meta: TableMeta {
                    schema: Arc::new(TableSchema::new(vec![TableField::new(
                        "a",
                        TableDataType::Number(NumberDataType::UInt64),
                    )])),
                    engine: "FUSE".to_string(),
                    ..Default::default()
                },
                catalog_info: Arc::new(CatalogInfo::default()),
                db_type: DatabaseType::NormalDB,
            },
        })
    }

    fn add_table(metadata: &mut Metadata, table: Arc<dyn Table>) -> usize {
        metadata.add_table(
            "default".to_string(),
            "default".to_string(),
            table,
            None,
            None,
            false,
            false,
            false,
            None,
        )
    }

    fn scan_expr(metadata: &Metadata, table_index: usize) -> SExpr {
        let columns = metadata
            .columns_by_table_index(table_index)
            .into_iter()
            .map(|column| column.index())
            .collect();
        SExpr::create_leaf(Arc::new(RelOperator::Scan(Scan {
            table_index,
            columns,
            ..Default::default()
        })))
    }

    fn cross_join_expr(left: SExpr, right: SExpr) -> SExpr {
        SExpr::create_binary(
            Arc::new(RelOperator::Join(Join {
                join_type: JoinType::Cross,
                ..Default::default()
            })),
            Arc::new(left),
            Arc::new(right),
        )
    }

    #[test]
    fn test_analyze_common_subexpression_prefers_cross_join_subtree() {
        let mut metadata = Metadata::default();
        let t1 = fake_fuse_table(1, "t1");
        let t2 = fake_fuse_table(2, "t2");

        let t1_left = add_table(&mut metadata, t1.clone());
        let t2_left = add_table(&mut metadata, t2.clone());
        let t1_right = add_table(&mut metadata, t1);
        let t2_right = add_table(&mut metadata, t2);

        let left = cross_join_expr(scan_expr(&metadata, t1_left), scan_expr(&metadata, t2_left));
        let right = cross_join_expr(
            scan_expr(&metadata, t1_right),
            scan_expr(&metadata, t2_right),
        );
        let root = cross_join_expr(left, right);

        let (replacements, materialized_ctes) =
            analyze_common_subexpression(&root, &mut metadata).unwrap();

        assert_eq!(replacements.len(), 2);
        assert_eq!(materialized_ctes.len(), 1);

        let cte_def = materialized_ctes[0].child(0).unwrap();
        let RelOperator::Join(join) = cte_def.plan() else {
            panic!(
                "expected cross join materialized cte, got {:?}",
                cte_def.plan()
            );
        };
        assert_eq!(join.join_type, JoinType::Cross);
    }

    #[test]
    fn test_analyze_common_subexpression_keeps_cross_join_operand_order() {
        let mut metadata = Metadata::default();
        let t1 = fake_fuse_table(1, "t1");
        let t2 = fake_fuse_table(2, "t2");

        let t1_left = add_table(&mut metadata, t1.clone());
        let t2_left = add_table(&mut metadata, t2.clone());
        let t1_right = add_table(&mut metadata, t1);
        let t2_right = add_table(&mut metadata, t2);

        let left = cross_join_expr(scan_expr(&metadata, t1_left), scan_expr(&metadata, t2_left));
        let right = cross_join_expr(
            scan_expr(&metadata, t2_right),
            scan_expr(&metadata, t1_right),
        );
        let root = cross_join_expr(left, right);

        let (_replacements, materialized_ctes) =
            analyze_common_subexpression(&root, &mut metadata).unwrap();

        assert_eq!(materialized_ctes.len(), 2);
        assert!(
            materialized_ctes
                .iter()
                .all(|cte| matches!(cte.child(0).unwrap().plan(), RelOperator::Scan(_)))
        );
    }
}

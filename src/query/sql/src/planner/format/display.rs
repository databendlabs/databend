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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;
use crate::ScalarExpr;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatInfo;
use crate::plans::RelOperator;

/// A trait for humanizing IDs.
pub trait IdHumanizer {
    fn humanize_column_id(&self, id: IndexType) -> String;

    fn humanize_table_id(&self, id: IndexType) -> String;

    fn options(&self) -> &FormatOptions;
}

/// A trait for humanizing operators.
pub trait OperatorHumanizer<I: IdHumanizer> {
    fn humanize_operator(&self, id_humanizer: &I, op: &RelOperator) -> FormatTreeNode;
}

#[derive(Debug, Clone, Default)]
pub struct FormatOptions {
    pub verbose: bool,
}

/// A default implementation of `OperatorHumanizer`.
/// It will use `IdHumanizer` to humanize the IDs and then print the operator.
///
/// Although the `Output` is `FormatTreeNode`, the children of the operator are not filled.
///
/// # Example
/// ```
/// use databend_query::planner::format::display_rel_operator::DefaultIdHumanizer;
/// use databend_query::planner::format::display_rel_operator::DefaultOperatorHumanizer;
/// use databend_query::planner::format::display_rel_operator::Filter;
/// use databend_query::planner::format::display_rel_operator::IdHumanizer;
/// use databend_query::planner::format::display_rel_operator::OperatorHumanizer;
/// use databend_query::planner::format::display_rel_operator::RelOperator;
///
/// let id_humanizer = DefaultIdHumanizer;
/// let operator_humanizer = DefaultOperatorHumanizer;
/// let op = RelOperator::Filter(Filter { predicates: vec![] });
/// let tree = operator_humanizer.humanize_operator(&id_humanizer, &op);
///
/// assert_eq!(tree.payload, "Filter");
/// assert_eq!(tree.children.len(), 1);
/// ```
pub struct DefaultOperatorHumanizer;

pub struct MetadataIdHumanizer<'a> {
    metadata: &'a Metadata,
    options: FormatOptions,
}

impl<'a> MetadataIdHumanizer<'a> {
    pub fn new(metadata: &'a Metadata, options: FormatOptions) -> Self {
        Self { metadata, options }
    }
}

impl IdHumanizer for MetadataIdHumanizer<'_> {
    fn humanize_column_id(&self, id: IndexType) -> String {
        let column_entry = self.metadata.column(id);
        match column_entry {
            ColumnEntry::BaseTableColumn(column) => {
                let table = self.metadata.table(column.table_index);
                let table = table.name();
                let column = column.column_name.as_str();
                format!("{table}.{column} (#{id})")
            }
            ColumnEntry::DerivedColumn(column) => {
                let alias = &column.alias;
                format!("{alias} (#{id})")
            }
            ColumnEntry::InternalColumn(column) => {
                let column = column.internal_column.column_name.as_str();
                format!("internal.{column} (#{id})")
            }
            ColumnEntry::VirtualColumn(column) => {
                let table = self.metadata.table(column.table_index);
                let db = table.database();
                let table = table.name();
                let column = column.column_name.as_str();
                format!("{db}.{table}.{column} (#{id})")
            }
        }
    }

    fn humanize_table_id(&self, id: IndexType) -> String {
        let table = self.metadata.table(id);
        let db = table.database();
        let table = table.name();
        format!("{}.{} (#{})", db, table, id)
    }

    fn options(&self) -> &FormatOptions {
        &self.options
    }
}

/// A humanizer for `SExpr`.
/// It will use `IdHumanizer` and `OperatorHumanizer` to humanize the `SExpr`.
/// The result is a `FormatTreeNode` with the operator and its children.
pub struct TreeHumanizer<'a, I, O> {
    id_humanizer: &'a I,
    operator_humanizer: &'a O,
}

impl<'a, I, O> TreeHumanizer<'a, I, O>
where
    I: IdHumanizer,
    O: OperatorHumanizer<I>,
{
    pub fn new(id_humanizer: &'a I, operator_humanizer: &'a O) -> Self {
        TreeHumanizer {
            id_humanizer,
            operator_humanizer,
        }
    }

    pub fn humanize_s_expr(&self, s_expr: &SExpr) -> Result<FormatTreeNode> {
        let op = s_expr.plan();
        let mut tree = self
            .operator_humanizer
            .humanize_operator(self.id_humanizer, op);

        if self.id_humanizer.options().verbose {
            let rel_expr = RelExpr::with_s_expr(s_expr);
            let prop = rel_expr.derive_relational_prop()?;
            let stat = rel_expr.derive_cardinality()?;
            let properties = self.humanize_property(&prop);
            let stats = self.humanize_stat(&stat)?;
            tree.children.extend(properties);
            tree.children.extend(stats);
        }

        let subquerys = op.collect_subquery();
        if !subquerys.is_empty() {
            let subquerys = subquerys
                .into_iter()
                .map(|subquery| {
                    let children = vec![
                        subquery
                            .compare_op
                            .map(|compare| FormatTreeNode::new(format!("compare_op: {compare:?}"))),
                        Some(FormatTreeNode::new(format!(
                            "output_column: {}",
                            self.id_humanizer
                                .humanize_column_id(subquery.output_column.index)
                        ))),
                        subquery.projection_index.map(|id| {
                            FormatTreeNode::new(format!(
                                "projection_index: {}",
                                self.id_humanizer.humanize_column_id(id)
                            ))
                        }),
                        Some(subquery.subquery.to_format_tree(self.id_humanizer)?),
                    ];
                    Ok(FormatTreeNode::with_children(
                        format!("Subquery ({:?})", subquery.typ),
                        children.into_iter().flatten().collect(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            tree.children.push(FormatTreeNode::with_children(
                "subquerys".to_string(),
                subquerys,
            ));
        }

        let children = s_expr
            .children()
            .map(|s_expr| self.humanize_s_expr(s_expr))
            .collect::<Result<Vec<_>>>()?;

        tree.children.extend(children);
        Ok(tree)
    }

    fn humanize_property(&self, prop: &RelationalProperty) -> Vec<FormatTreeNode> {
        let output_columns = prop
            .output_columns
            .iter()
            .map(|idx| self.id_humanizer.humanize_column_id(*idx))
            .sorted()
            .collect::<Vec<_>>();

        let outer_columns = prop
            .outer_columns
            .iter()
            .map(|idx| self.id_humanizer.humanize_column_id(*idx))
            .sorted()
            .collect::<Vec<_>>();

        let used_columns = prop
            .used_columns
            .iter()
            .map(|idx| self.id_humanizer.humanize_column_id(*idx))
            .sorted()
            .collect::<Vec<_>>();

        vec![
            FormatTreeNode::new(format!("output columns: [{}]", output_columns.join(", "))),
            FormatTreeNode::new(format!("outer columns: [{}]", outer_columns.join(", "))),
            FormatTreeNode::new(format!("used columns: [{}]", used_columns.join(", "))),
        ]
    }

    fn humanize_stat(&self, stat: &StatInfo) -> Result<Vec<FormatTreeNode>> {
        let cardinality = format!("{:.3}", stat.cardinality);

        let precise_cardinality = if let Some(card) = stat.statistics.precise_cardinality {
            format!("{}", card)
        } else {
            "N/A".to_string()
        };

        let column_stats = stat
            .statistics
            .column_stats
            .iter()
            .map(|(column, hist)| {
                let column = self.id_humanizer.humanize_column_id(*column);
                let hist = format!(
                    "{{ min: {}, max: {}, ndv: {}, null count: {} }}",
                    hist.min,
                    hist.max,
                    hist.ndv.value(),
                    hist.null_count
                );
                FormatTreeNode::new(format!("{}: {}", column, hist))
            })
            .sorted_by(|a, b| a.payload.cmp(&b.payload))
            .collect::<Vec<_>>();

        Ok(vec![
            FormatTreeNode::new(format!("cardinality: {}", cardinality)),
            FormatTreeNode::new(format!("precise cardinality: {}", precise_cardinality)),
            FormatTreeNode::with_children("statistics".to_string(), column_stats),
        ])
    }
}

pub fn format_scalar(scalar: &ScalarExpr) -> String {
    match scalar {
        ScalarExpr::BoundColumnRef(column_ref) => {
            if let Some(table_name) = &column_ref.column.table_name {
                format!(
                    "{}.{} (#{})",
                    table_name, column_ref.column.column_name, column_ref.column.index
                )
            } else {
                format!(
                    "{} (#{})",
                    column_ref.column.column_name, column_ref.column.index
                )
            }
        }
        ScalarExpr::ConstantExpr(constant) => constant.value.to_string(),
        ScalarExpr::TypedConstantExpr(constant, _) => constant.value.to_string(),
        ScalarExpr::WindowFunction(win) => win.display_name.clone(),
        ScalarExpr::AggregateFunction(agg) => agg.display_name.clone(),
        ScalarExpr::LambdaFunction(lambda) => {
            let args = lambda
                .args
                .iter()
                .map(format_scalar)
                .collect::<Vec<String>>()
                .join(", ");
            format!(
                "{}({}, {})",
                &lambda.func_name, args, &lambda.lambda_display,
            )
        }
        ScalarExpr::FunctionCall(func) => {
            format!(
                "{}({})",
                &func.func_name,
                func.arguments
                    .iter()
                    .map(format_scalar)
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::CastExpr(cast) => {
            format!(
                "CAST({} AS {})",
                format_scalar(&cast.argument),
                cast.target_type
            )
        }
        ScalarExpr::SubqueryExpr(_) => "SUBQUERY".to_string(),
        ScalarExpr::UDFCall(udf) => {
            format!(
                "{}({})",
                &udf.handler,
                udf.arguments
                    .iter()
                    .map(format_scalar)
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        ScalarExpr::UDFLambdaCall(udf) => {
            format!("{}({})", &udf.func_name, format_scalar(&udf.scalar))
        }
        ScalarExpr::UDAFCall(udaf) => udaf.display_name.clone(),
        ScalarExpr::AsyncFunctionCall(async_func) => async_func.display_name.clone(),
    }
}

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

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::Unpivot;
use databend_common_exception::Result;

use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnSet;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;

impl Binder {
    pub(crate) fn bind_unpivot(
        &mut self,
        s_expr: SExpr,
        mut bind_context: BindContext,
        unpivot: &Unpivot,
    ) -> Result<(SExpr, BindContext)> {
        let targets = Self::unpivot_targets(unpivot);
        let mut generated = self.normalize_select_list(&mut bind_context, &targets)?;
        self.analyze_project_set_select(&mut bind_context, &mut generated)?;

        let mut source_indices = ColumnSet::new();
        for srf in &bind_context.srf_info.srfs {
            srf.scalar.collect_used_columns(&mut source_indices);
        }
        let (database_name, table_name, table_index) =
            Self::common_source_identity(&bind_context.columns, &source_indices);

        let s_expr = self.bind_project_set(&mut bind_context, s_expr, false)?;
        bind_context.srf_info = Default::default();
        bind_context
            .columns
            .retain(|column| !source_indices.contains(&column.index));

        let mut items = Vec::with_capacity(generated.items.len());
        for item in generated.items {
            let data_type = item.scalar.data_type().into_owned();
            let index = self
                .metadata
                .write()
                .add_derived_column(item.alias.clone(), data_type.clone());
            let column = ColumnBindingBuilder::new(
                item.alias,
                index,
                Box::new(data_type),
                Visibility::Visible,
            )
            .database_name(database_name.clone())
            .table_name(table_name.clone())
            .table_index(table_index)
            .build();
            items.push(ScalarItem {
                scalar: item.scalar,
                index,
            });
            bind_context.add_column_binding(column);
        }

        let eval_scalar = EvalScalar { items };
        let s_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(s_expr));
        Ok((s_expr, bind_context))
    }

    fn unpivot_targets(unpivot: &Unpivot) -> [SelectTarget; 2] {
        let name_values = Expr::Array {
            span: Span::default(),
            exprs: unpivot
                .column_names
                .iter()
                .map(|name| Expr::Literal {
                    span: name.ident.span,
                    value: Literal::String(
                        name.alias.as_ref().unwrap_or(&name.ident.name).to_string(),
                    ),
                })
                .collect(),
        };
        let column_values = Expr::Array {
            span: Span::default(),
            exprs: unpivot
                .column_names
                .iter()
                .map(|name| Expr::ColumnRef {
                    span: None,
                    column: ColumnRef {
                        database: None,
                        table: None,
                        column: ColumnID::Name(name.ident.clone()),
                    },
                })
                .collect(),
        };

        [
            Self::unpivot_target(name_values, unpivot.unpivot_column.clone()),
            Self::unpivot_target(column_values, unpivot.value_column.clone()),
        ]
    }

    fn unpivot_target(argument: Expr, alias: Identifier) -> SelectTarget {
        SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall {
                span: Span::default(),
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(Span::default(), "unnest"),
                    args: vec![argument],
                    params: vec![],
                    order_by: vec![],
                    filter: None,
                    window: None,
                    lambda: None,
                },
            }),
            alias: Some(alias),
        }
    }

    fn common_source_identity(
        columns: &[ColumnBinding],
        source_indices: &ColumnSet,
    ) -> (Option<String>, Option<String>, Option<crate::IndexType>) {
        let Some(first) = columns
            .iter()
            .find(|column| source_indices.contains(&column.index))
        else {
            return (None, None, None);
        };
        let database_name = columns
            .iter()
            .filter(|column| source_indices.contains(&column.index))
            .all(|column| column.database_name == first.database_name)
            .then(|| first.database_name.clone())
            .flatten();
        let table_name = columns
            .iter()
            .filter(|column| source_indices.contains(&column.index))
            .all(|column| column.table_name == first.table_name)
            .then(|| first.table_name.clone())
            .flatten();
        let table_index = columns
            .iter()
            .filter(|column| source_indices.contains(&column.index))
            .all(|column| column.table_index == first.table_index)
            .then_some(first.table_index)
            .flatten();
        (database_name, table_name, table_index)
    }
}

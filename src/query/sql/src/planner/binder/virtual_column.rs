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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_license::license::Feature::VirtualColumn;
use common_license::license_manager::get_license_manager;
use jsonb::keypath::parse_key_paths;
use jsonb::keypath::KeyPath;

use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

pub(crate) struct VirtualColumnRewriter {
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,

    /// Mapping: (table index) -> (derived virtual column indices)
    /// This is used to add virtual column indices to Scan plan
    table_virtual_columns: HashMap<IndexType, Vec<IndexType>>,
}

impl VirtualColumnRewriter {
    pub(crate) fn new(ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            ctx,
            metadata,
            table_virtual_columns: Default::default(),
        }
    }

    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let license_manager = get_license_manager();
        if license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), VirtualColumn)
            .is_err()
        {
            return Ok(s_expr.clone());
        }
        self.rewrite_virtual_column(s_expr)
    }

    fn rewrite_virtual_column(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();

        // Rewrite variant inner column as derived Virtual Column.
        match (*s_expr.plan).clone() {
            RelOperator::Scan(mut scan) => {
                let virtual_indices = self.table_virtual_columns.get(&scan.table_index);
                if let Some(indices) = virtual_indices {
                    for index in indices {
                        scan.columns.insert(*index);
                    }
                    s_expr.plan = Arc::new(scan.into());
                }
            }
            RelOperator::EvalScalar(mut eval_scalar) => {
                for item in &mut eval_scalar.items {
                    self.visit(&mut item.scalar)?;
                }
                s_expr.plan = Arc::new(eval_scalar.into());
            }
            RelOperator::Filter(mut filter) => {
                for scalar in &mut filter.predicates {
                    self.visit(scalar)?;
                }
                s_expr.plan = Arc::new(filter.into());
            }
            RelOperator::ProjectSet(mut project_set) => {
                for item in &mut project_set.srfs {
                    self.visit(&mut item.scalar)?;
                }
                s_expr.plan = Arc::new(project_set.into());
            }
            _ => {}
        }

        if !s_expr.children.is_empty() {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(self.rewrite_virtual_column(child)?));
            }
            s_expr.children = children;
        }

        Ok(s_expr)
    }
}

impl<'a> VisitorMut<'a> for VirtualColumnRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::FunctionCall(FunctionCall {
                func_name,
                arguments,
                ..
            }) if func_name == "get_by_keypath" && arguments.len() == 2 => {
                if let (
                    ScalarExpr::BoundColumnRef(column_ref),
                    ScalarExpr::ConstantExpr(constant),
                ) = (arguments[0].clone(), arguments[1].clone())
                {
                    let column_entry = self.metadata.read().column(column_ref.column.index).clone();
                    if let ColumnEntry::BaseTableColumn(base_column) = column_entry {
                        if !self
                            .metadata
                            .read()
                            .table(base_column.table_index)
                            .table()
                            .support_virtual_columns()
                            || base_column.data_type.remove_nullable() != TableDataType::Variant
                        {
                            return Ok(());
                        }
                        let name = match constant.value.clone() {
                            Scalar::String(v) => match parse_key_paths(&v) {
                                Ok(key_paths) => {
                                    let mut name = String::new();
                                    name.push_str(&base_column.column_name);
                                    for path in key_paths.paths {
                                        match path {
                                            KeyPath::Index(idx) => {
                                                name.push('[');
                                                name.push_str(&idx.to_string());
                                                name.push(']');
                                            }
                                            KeyPath::QuotedName(field) | KeyPath::Name(field) => {
                                                name.push(':');
                                                name.push_str(field.as_ref());
                                            }
                                        }
                                    }
                                    name
                                }
                                Err(_) => {
                                    return Ok(());
                                }
                            },
                            _ => {
                                return Ok(());
                            }
                        };

                        let mut index = 0;
                        // Check for duplicate virtual columns
                        for table_column in self
                            .metadata
                            .read()
                            .virtual_columns_by_table_index(base_column.table_index)
                        {
                            if table_column.name() == name {
                                index = table_column.index();
                                break;
                            }
                        }
                        if index == 0 {
                            let table_data_type =
                                TableDataType::Nullable(Box::new(TableDataType::Variant));
                            index = self.metadata.write().add_virtual_column(
                                base_column.table_index,
                                base_column.column_name.clone(),
                                base_column.column_index,
                                name.clone(),
                                table_data_type,
                                constant.value.clone(),
                            );
                        }

                        if let Some(indices) =
                            self.table_virtual_columns.get_mut(&base_column.table_index)
                        {
                            indices.push(index);
                        } else {
                            self.table_virtual_columns
                                .insert(base_column.table_index, vec![index]);
                        }

                        let column_binding = ColumnBindingBuilder::new(
                            name,
                            index,
                            Box::new(DataType::Nullable(Box::new(DataType::Variant))),
                            Visibility::InVisible,
                        )
                        .table_index(Some(base_column.table_index))
                        .build();

                        let replaced_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: column_binding,
                        });
                        *expr = replaced_column;
                        return Ok(());
                    }
                }
            }
            _ => {}
        }
        walk_expr_mut(self, expr)?;

        Ok(())
    }
}

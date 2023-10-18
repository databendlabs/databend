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

use chrono_tz::Tz;
use common_ast::ast::AddColumnOption;
use common_ast::ast::AlterTableAction;
use common_ast::ast::AlterTableStmt;
use common_ast::ast::ColumnDefinition;
use common_ast::ast::Identifier;
use common_ast::ast::InsertOperation;
use common_ast::ast::InsertSource;
use common_ast::ast::InsertStmt;
use common_ast::ast::MatchOperation;
use common_ast::ast::MatchedClause;
use common_ast::ast::MergeIntoStmt;
use common_ast::ast::MergeOption;
use common_ast::ast::MergeSource;
use common_ast::ast::MergeUpdateExpr;
use common_ast::ast::NullableConstraint;
use common_ast::ast::Statement;
use common_ast::ast::TableReference;
use common_ast::ast::UnmatchedClause;
use common_ast::ast::UpdateExpr;
use common_ast::ast::UpdateStmt;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ScalarRef;
use common_expression::TableField;
use common_formats::field_encoder::FieldEncoderRowBased;
use common_formats::field_encoder::FieldEncoderValues;
use common_formats::CommonSettings;
use common_io::constants::FALSE_BYTES_LOWER;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_UPPER;
use common_io::constants::TRUE_BYTES_LOWER;
use common_sql::resolve_type_name;
use itertools::join;
use rand::Rng;
use roaring::RoaringTreemap;

use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

enum MutTableAction {
    RenameTable(String),
    AddColumn((AddColumnOption, ColumnDefinition)),
    RenameColumn((Identifier, Identifier)),
    ModifyColumnDataType(ColumnDefinition),
    DropColumn(Identifier),
}

impl<'a, R: Rng + 'a> SqlGenerator<'a, R> {
    pub(crate) fn gen_insert(&mut self, table: &Table, row_count: usize) -> InsertStmt {
        let table_name = Identifier::from_name(table.name.clone());
        let data_types = table
            .schema
            .fields()
            .iter()
            .map(|f| (&f.data_type).into())
            .collect::<Vec<DataType>>();
        let source = self.gen_insert_source(&data_types, row_count);

        InsertStmt {
            // TODO
            hints: None,
            catalog: None,
            database: None,
            table: table_name,
            // TODO
            columns: vec![],
            source,
            // TODO
            overwrite: false,
        }
    }

    pub(crate) fn gen_delete(&mut self) -> Statement {
        let idx = self.rng.gen_range(0..self.tables.len());
        let table = self.tables[idx].clone();

        let table_reference = TableReference::Table {
            span: Span::default(),
            catalog: None,
            database: None,
            table: Identifier::from_name(table.name),
            alias: None,
            travel_point: None,
            pivot: None,
            unpivot: None,
        };
        let selection = Some(self.gen_expr(&DataType::Boolean));

        Statement::Delete {
            hints: None,
            table_reference,
            selection,
        }
    }

    pub(crate) fn gen_update(&mut self) -> UpdateStmt {
        let idx = self.rng.gen_range(0..self.tables.len());
        let table = self.tables[idx].clone();

        let table_reference = TableReference::Table {
            span: Span::default(),
            catalog: None,
            database: None,
            table: Identifier::from_name(table.name.clone()),
            alias: None,
            travel_point: None,
            pivot: None,
            unpivot: None,
        };
        let selection = if self.rng.gen_bool(0.8) {
            None
        } else {
            Some(self.gen_expr(&DataType::Boolean))
        };

        let mut fields = table
            .schema
            .fields
            .iter()
            .filter(|_| self.rng.gen_bool(0.1))
            .collect::<Vec<_>>();
        if fields.is_empty() {
            fields = vec![table.schema.field(0)];
        }
        let mut update_list = Vec::with_capacity(fields.len());
        for field in fields {
            update_list.push(UpdateExpr {
                name: Identifier::from_name(field.name().clone()),
                expr: self.gen_scalar_value(&DataType::from(field.data_type())),
            });
        }
        UpdateStmt {
            hints: None,
            table: table_reference,
            update_list,
            selection,
        }
    }

    pub(crate) fn gen_merge(&mut self) -> MergeIntoStmt {
        self.cte_tables.clear();
        self.bound_tables.clear();
        self.bound_columns.clear();
        self.is_join = false;

        let idx = self.rng.gen_range(0..self.tables.len());
        let table = self.tables[idx].clone();
        self.bound_table(table.clone());

        let (query, schema) = self.gen_subquery(false);
        let (source_table, source_alias) = self.gen_subquery_table(schema);
        self.bound_table(source_table);
        let source = MergeSource::Select {
            query: Box::new(query),
            source_alias,
        };

        self.only_scalar_expr = true;
        let join_expr = self.gen_binary_expr();

        let opt_num = self.rng.gen_range(1..=5);
        let mut merge_options = Vec::with_capacity(opt_num);
        for i in 0..opt_num {
            let selection = if i < (opt_num - 1) || self.rng.gen_bool(0.5) {
                self.only_scalar_expr = true;
                Some(self.gen_expr(&DataType::Boolean))
            } else {
                None
            };

            let merge_opt = if self.rng.gen_bool(0.5) {
                let operation = if self.rng.gen_bool(0.7) {
                    let mut fields = table
                        .schema
                        .fields
                        .iter()
                        .filter(|_| self.rng.gen_bool(0.1))
                        .collect::<Vec<_>>();
                    if fields.is_empty() {
                        fields = vec![table.schema.field(0)];
                    }

                    let mut update_list = Vec::with_capacity(fields.len());
                    for field in fields {
                        self.only_scalar_expr = true;
                        let update_expr = MergeUpdateExpr {
                            catalog: None,
                            table: None,
                            name: Identifier::from_name(field.name().clone()),
                            expr: self.gen_expr(&DataType::from(field.data_type())),
                        };
                        update_list.push(update_expr);
                    }
                    // TODO: is_star true
                    MatchOperation::Update {
                        update_list,
                        is_star: false,
                    }
                } else {
                    MatchOperation::Delete
                };

                MergeOption::Match(MatchedClause {
                    selection,
                    operation,
                })
            } else {
                let (columns, values) = if self.rng.gen_bool(0.5) {
                    let fields = table
                        .schema
                        .fields
                        .iter()
                        .filter(|_| self.rng.gen_bool(0.2))
                        .collect::<Vec<_>>();

                    let columns = fields
                        .iter()
                        .map(|f| Identifier::from_name(f.name()))
                        .collect::<Vec<_>>();
                    let values = fields
                        .iter()
                        .map(|f| {
                            let ty = DataType::from(f.data_type());
                            self.gen_scalar_value(&ty)
                        })
                        .collect::<Vec<_>>();

                    (Some(columns), values)
                } else {
                    let values = table
                        .schema
                        .fields
                        .iter()
                        .map(|f| {
                            let ty = DataType::from(f.data_type());
                            self.gen_scalar_value(&ty)
                        })
                        .collect::<Vec<_>>();
                    (None, values)
                };

                let insert_operation = InsertOperation {
                    columns,
                    values,
                    is_star: false,
                };
                MergeOption::Unmatch(UnmatchedClause {
                    selection,
                    insert_operation,
                })
            };
            merge_options.push(merge_opt);
        }

        MergeIntoStmt {
            hints: None,
            catalog: None,
            database: None,
            table_ident: Identifier::from_name(table.name),
            source,
            target_alias: None,
            join_expr,
            merge_options,
        }
    }

    fn is_column_not_null(column: &ColumnDefinition) -> bool {
        match column.nullable_constraint {
            Some(NullableConstraint::NotNull) => true,
            Some(NullableConstraint::Null) => false,
            None => true,
        }
    }

    fn random_select_field(&mut self, table: &Table) -> TableField {
        let field_index = self.rng.gen_range(0..table.schema.num_fields());
        table.schema.fields[field_index].clone()
    }

    fn from_column_to_field(column: &ColumnDefinition) -> TableField {
        let not_null = Self::is_column_not_null(column);
        let data_type = resolve_type_name(&column.data_type, not_null).unwrap();
        TableField::new(&column.name.name, data_type)
    }

    fn mut_table(table: &mut Table, action: MutTableAction) {
        let mut new_schema = table.schema.as_ref().clone();
        match action {
            MutTableAction::RenameTable(name) => table.name = name,
            MutTableAction::AddColumn((add_column_option, column)) => {
                let field = Self::from_column_to_field(&column);

                match add_column_option {
                    AddColumnOption::End => new_schema.fields.push(field),
                    AddColumnOption::First => new_schema.fields.insert(0, field),
                    AddColumnOption::After(after_column) => {
                        let field_index = new_schema.index_of(&after_column.name).unwrap() + 1;
                        new_schema.fields.insert(field_index, field);
                    }
                }
            }
            MutTableAction::RenameColumn((old_column, new_column)) => {
                let field_index = new_schema.column_id_of(&old_column.name).unwrap();
                let field = &mut new_schema.fields[field_index as usize];
                field.name = new_column.name;
            }
            MutTableAction::ModifyColumnDataType(column) => {
                let field_index = new_schema.index_of(&column.name.name).unwrap();
                let field = &mut new_schema.fields[field_index];
                let new_field = Self::from_column_to_field(&column);
                field.data_type = new_field.data_type;
            }
            MutTableAction::DropColumn(column) => {
                let field_index = new_schema.index_of(&column.name).unwrap();
                new_schema.fields.remove(field_index);
            }
        }

        table.schema = Arc::new(new_schema);
    }

    // generate alter table statement, new table schema and insert statement of new column(if any)
    pub(crate) fn gen_alter(
        &mut self,
        table: &Table,
        row_count: usize,
    ) -> Option<(AlterTableStmt, Table, Option<InsertStmt>)> {
        if self.rng.gen_bool(0.3) {
            return None;
        }
        let mut new_table = table.clone();
        let (action, new_column, mut_action) = match self.rng.gen_range(0..=4) {
            0 => {
                let new_table_name =
                    format!("{}_{}", table.name.clone(), self.rng.gen_range(0..10));
                (
                    AlterTableAction::RenameTable {
                        new_table: Identifier::from_name(new_table_name.clone()),
                    },
                    None,
                    MutTableAction::RenameTable(new_table_name),
                )
            }
            1 => {
                let column = self.gen_new_column();
                let option = match self.rng.gen_range(0..=2) {
                    0 => AddColumnOption::End,
                    1 => AddColumnOption::First,
                    2 => {
                        let field = self.random_select_field(table);
                        let column = Identifier::from_name(field.name);
                        AddColumnOption::After(column)
                    }
                    _ => unreachable!(),
                };
                (
                    AlterTableAction::AddColumn {
                        column: column.clone(),
                        option: option.clone(),
                    },
                    Some(column.clone()),
                    MutTableAction::AddColumn((option, column)),
                )
            }
            2 => {
                let field = self.random_select_field(table);
                let old_column = Identifier::from_name(field.name);
                let new_column = self.gen_new_column().name;
                (
                    AlterTableAction::RenameColumn {
                        old_column: old_column.clone(),
                        new_column: new_column.clone(),
                    },
                    None,
                    MutTableAction::RenameColumn((old_column, new_column)),
                )
            }
            3 => {
                let field = self.random_select_field(table);
                let name = Identifier::from_name(field.name);
                let (data_type, nullable_constraint) = self.gen_data_type_name(None);
                let new_column = ColumnDefinition {
                    name,
                    data_type,
                    expr: None,
                    comment: None,
                    nullable_constraint,
                };
                (
                    AlterTableAction::ModifyColumn {
                        action: common_ast::ast::ModifyColumnAction::SetDataType(vec![
                            new_column.clone(),
                        ]),
                    },
                    Some(new_column.clone()),
                    MutTableAction::ModifyColumnDataType(new_column),
                )
            }
            4 => {
                let field = self.random_select_field(table);
                let column = Identifier::from_name(field.name);
                (
                    AlterTableAction::DropColumn {
                        column: column.clone(),
                    },
                    None,
                    MutTableAction::DropColumn(column),
                )
            }
            _ => unreachable!(),
        };

        Self::mut_table(&mut new_table, mut_action);

        let insert_stmt_opt = if let Some(new_column) = new_column {
            let not_null = Self::is_column_not_null(&new_column);
            let table_name = Identifier::from_name(table.name.clone());
            let columns = vec![new_column.name.clone()];
            let data_type = resolve_type_name(&new_column.data_type, not_null).unwrap();
            let data_types = vec![(&data_type).into()];
            let source = self.gen_insert_source(&data_types, row_count);

            Some(InsertStmt {
                // TODO
                hints: None,
                catalog: None,
                database: None,
                table: table_name,
                columns,
                source,
                overwrite: false,
            })
        } else {
            None
        };

        let table_reference = TableReference::Table {
            span: Span::default(),
            catalog: None,
            database: None,
            table: Identifier::from_name(table.name.clone()),
            alias: None,
            travel_point: None,
            pivot: None,
            unpivot: None,
        };
        Some((
            AlterTableStmt {
                if_exists: true,
                action,
                table_reference,
            },
            new_table,
            insert_stmt_opt,
        ))
    }

    fn gen_insert_source(&mut self, data_types: &[DataType], row_count: usize) -> InsertSource {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let columns = self.gen_columns(data_types, row_count);
                let mut buf = Vec::new();
                let encoder = FieldEncoderValues {
                    common_settings: CommonSettings {
                        true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                        false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                        null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                        nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                        inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                        timezone: Tz::UTC,
                        disable_variant_check: false,
                    },
                    quote_char: b'\'',
                };

                for i in 0..row_count {
                    if i > 0 {
                        buf.extend_from_slice(b",");
                    }
                    buf.extend_from_slice(b"(");
                    for (j, column) in columns.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b",");
                        }
                        if column.data_type().remove_nullable() == DataType::Bitmap {
                            // convert binary bitmap to string
                            match unsafe { column.index_unchecked(i) } {
                                ScalarRef::Null => {
                                    buf.extend_from_slice(NULL_BYTES_UPPER.as_bytes());
                                }
                                ScalarRef::Bitmap(v) => {
                                    let rb = RoaringTreemap::deserialize_from(v).unwrap();
                                    let vals = rb.into_iter().collect::<Vec<_>>();
                                    let s = join(vals.iter(), ",");
                                    buf.push(b'\'');
                                    buf.extend_from_slice(s.as_bytes());
                                    buf.push(b'\'');
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            encoder.write_field(column, i, &mut buf, false);
                        }
                    }
                    buf.extend_from_slice(b")");
                }
                InsertSource::Values {
                    rest_str: unsafe { String::from_utf8_unchecked(buf) },
                    start: 0,
                }
            }
            // TODO
            _ => unreachable!(),
        }
    }

    fn gen_columns(&mut self, data_types: &[DataType], row_count: usize) -> Vec<Column> {
        data_types
            .iter()
            .map(|ty| Column::random(ty, row_count))
            .collect()
    }
}

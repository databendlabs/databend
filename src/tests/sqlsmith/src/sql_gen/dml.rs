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

use chrono_tz::Tz;
use common_ast::ast::AddColumnOption;
use common_ast::ast::AlterTableAction;
use common_ast::ast::AlterTableStmt;
use common_ast::ast::ColumnDefinition;
use common_ast::ast::Identifier;
use common_ast::ast::InsertSource;
use common_ast::ast::InsertStmt;
use common_ast::ast::NullableConstraint;
use common_ast::ast::Statement;
use common_ast::ast::TableReference;
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

impl<'a, R: Rng> SqlGenerator<'a, R> {
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

    pub(crate) fn gen_delete(&mut self, table: &Table) -> Statement {
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
        let selection = if self.rng.gen_bool(0.5) {
            None
        } else {
            Some(self.gen_expr(&DataType::Boolean))
        };

        Statement::Delete {
            hints: None,
            table_reference,
            selection,
        }
    }

    pub(crate) fn gen_update(&mut self, table: &Table) -> UpdateStmt {
        let data_types = table
            .schema
            .fields()
            .iter()
            .map(|f| (Identifier::from_name(&f.name), (&f.data_type).into()))
            .collect::<Vec<(Identifier, DataType)>>();
        let table = TableReference::Table {
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

        let update_list = {
            let mut res = vec![];
            for (col_name, ty) in data_types.iter().take(self.rng.gen_range(1..=5)) {
                res.push(UpdateExpr {
                    name: col_name.clone(),
                    expr: self.gen_scalar_value(ty),
                });
            }
            res
        };
        UpdateStmt {
            hints: None,
            table,
            update_list,
            selection,
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

    // generate alter table statement, and insert statement of new column(if any)
    pub(crate) fn gen_alter(
        &mut self,
        table: &Table,
        row_count: usize,
    ) -> Option<(AlterTableStmt, Option<InsertStmt>)> {
        if self.rng.gen_bool(0.3) {
            return None;
        }

        let (action, new_column) = match self.rng.gen_range(0..=4) {
            0 => {
                let new_table = format!("{}_{}", table.name, self.rng.gen_range(0..10));
                (
                    AlterTableAction::RenameTable {
                        new_table: Identifier::from_name(new_table),
                    },
                    None,
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
                        option,
                    },
                    Some(column),
                )
            }
            2 => {
                let field = self.random_select_field(table);
                let old_column = Identifier::from_name(field.name);
                let new_column = self.gen_new_column().name;
                (
                    AlterTableAction::RenameColumn {
                        old_column,
                        new_column,
                    },
                    None,
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
                    Some(new_column),
                )
            }
            4 => {
                let field = self.random_select_field(table);
                let column = Identifier::from_name(field.name);
                (AlterTableAction::DropColumn { column }, None)
            }
            _ => unreachable!(),
        };

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

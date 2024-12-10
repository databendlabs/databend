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
use std::io;
use std::net::TcpListener;

use msql_srv::Column;
use msql_srv::ColumnFlags;
use msql_srv::ColumnType;
use msql_srv::MysqlIntermediary;
use msql_srv::MysqlShim;
use msql_srv::QueryResultWriter;
use msql_srv::StatementMetaWriter;
use mysql_common::Value;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Expr;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub fn run_mysql_source() {
    // Bind the listener to the address
    let listener = TcpListener::bind("0.0.0.0:3106").unwrap();

    let backend = Backend::create();
    loop {
        if let Ok((socket, _)) = listener.accept() {
            let backend = backend.clone();
            databend_common_base::runtime::Thread::spawn(move || {
                MysqlIntermediary::run_on_tcp(backend, socket).unwrap();
            });
        }
    }
}

// mock MySQL backend with a table `user`.
//
// CREATE TABLE `user`(
//   id INT,
//   name VARCHAR(100),
//   age SMALLINT UNSIGNED,
//   salary DOUBLE,
//   active BOOL
// );
//
// +------+-------+------+---------+--------+
// | id   | name  | age  | salary  | active |
// +------+-------+------+---------+--------+
// |    1 | Alice |   24 |     100 |      1 |
// |    2 | Bob   |   35 |   200.1 |      0 |
// |    3 | Lily  |   41 |  1000.2 |      1 |
// |    4 | Tom   |   55 | 3000.55 |      0 |
// +------+-------+------+---------+--------+
#[derive(Debug, Clone)]
struct Backend {
    table: String,
    schema: Vec<Column>,
    block: Vec<Vec<Option<Value>>>,

    prepared_id: u32,
    prepared: HashMap<u32, (usize, Vec<usize>, Vec<Expr>)>,
}

impl Backend {
    fn create() -> Self {
        let table = "user".to_string();

        let schema = vec![
            Column {
                table: "user".to_string(),
                column: "id".to_string(),
                coltype: ColumnType::MYSQL_TYPE_LONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "user".to_string(),
                column: "name".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "user".to_string(),
                column: "age".to_string(),
                coltype: ColumnType::MYSQL_TYPE_SHORT,
                colflags: ColumnFlags::UNSIGNED_FLAG,
            },
            Column {
                table: "user".to_string(),
                column: "salary".to_string(),
                coltype: ColumnType::MYSQL_TYPE_DOUBLE,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "user".to_string(),
                column: "active".to_string(),
                coltype: ColumnType::MYSQL_TYPE_TINY,
                colflags: ColumnFlags::empty(),
            },
        ];

        let block: Vec<Vec<Option<Value>>> = vec![
            vec![
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(3)),
                Some(Value::Int(4)),
                Some(Value::Int(5)),
            ],
            vec![
                Some(Value::Bytes("Alice".as_bytes().to_vec())),
                Some(Value::Bytes("Bob".as_bytes().to_vec())),
                Some(Value::Bytes("Lily".as_bytes().to_vec())),
                Some(Value::Bytes("Tom".as_bytes().to_vec())),
                None,
            ],
            vec![
                Some(Value::UInt(24)),
                Some(Value::UInt(35)),
                Some(Value::UInt(41)),
                Some(Value::UInt(55)),
                None,
            ],
            vec![
                Some(Value::Double(100.0)),
                Some(Value::Double(200.1)),
                Some(Value::Double(1000.20)),
                Some(Value::Double(3000.55)),
                None,
            ],
            vec![
                Some(Value::Int(1)),
                Some(Value::Int(0)),
                Some(Value::Int(1)),
                Some(Value::Int(0)),
                None,
            ],
        ];

        Self {
            table,
            schema,
            block,

            prepared_id: 0,
            prepared: HashMap::new(),
        }
    }
}

impl<W: io::Read + io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    fn on_prepare(&mut self, sql: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        let dialect = MySqlDialect {};
        let asts = Parser::parse_sql(&dialect, sql).unwrap();

        let mut table = None;
        let mut key = None;
        let mut values = vec![];
        let mut in_list_keys = vec![];

        // Only support SQL select two fields with an in expr.
        // for example: SELECT id, name FROM user WHERE id in (1,2,3,4,5);
        if asts.len() == 1 {
            if let Statement::Query(query) = &asts[0] {
                if let SetExpr::Select(select) = *query.body.clone() {
                    for proj in select.projection {
                        if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = &proj {
                            values.push(Some(ident.value.clone()));
                        }
                    }
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        table = Some(name.0[0].value.clone());
                    }
                    match &select.selection {
                        Some(Expr::InList { expr, list, .. }) => {
                            if let Expr::Identifier(ident) = *expr.clone() {
                                key = Some(ident.value.clone());
                                in_list_keys.extend(list.clone());
                            }
                        }
                        Some(Expr::BinaryOp { left, op, right }) => {
                            if op == &BinaryOperator::Eq {
                                if let Expr::Identifier(ident) = *left.clone() {
                                    key = Some(ident.value.clone());
                                    in_list_keys.push(*right.clone());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        self.prepared_id += 1;
        let prepared_id = self.prepared_id;

        if table.is_some() && key.is_some() && !values.is_empty() {
            let table = table.unwrap();

            let key = key.unwrap();
            let key_col = &self
                .schema
                .iter()
                .enumerate()
                .find(|&(_, f)| f.column == key);

            let mut value_columns = vec![];
            for value in values {
                let value = value.unwrap();
                let value_col = &self
                    .schema
                    .iter()
                    .enumerate()
                    .find(|&(_, f)| f.column == value);
                value_columns.push(*value_col);
            }

            if table == self.table && key_col.is_some() && !value_columns.is_empty() {
                let (key_idx, key_col) = key_col.unwrap();

                // keys are bind as string type.
                let mut key_col = key_col.clone();
                key_col.coltype = ColumnType::MYSQL_TYPE_VAR_STRING;

                let key_cols = vec![key_col];
                let mut value_cols = vec![];
                let mut value_idx_vec = vec![];
                for value_col in value_columns {
                    let (value_idx, value_col) = value_col.unwrap();
                    value_idx_vec.push(value_idx);
                    value_cols.push(value_col.clone());
                }
                let prepared_idices = (key_idx, value_idx_vec, in_list_keys);
                self.prepared.insert(prepared_id, prepared_idices);

                // add key and value columns for execute.
                return info.reply(prepared_id, key_cols.as_slice(), value_cols.as_slice());
            }
        }

        // ingore other unsupported SQLs.
        info.reply(prepared_id, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        _: msql_srv::ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let (key_idx, value_idx_vec, in_list_keys) = self.prepared.get(&id).unwrap();
        let key_field = self.schema[*key_idx].clone();
        let key_column = self.block[*key_idx].clone();

        // step-1: find matched rows by compare key params.
        let mut rows: Vec<Option<usize>> = vec![];
        match key_field.coltype {
            ColumnType::MYSQL_TYPE_TINY => {
                for param in in_list_keys {
                    let param = format!("{}", param);
                    if param == "NULL" {
                        continue;
                    }
                    let key = param.parse::<bool>().unwrap();
                    let key_param = Value::Int(key.into());
                    for (i, key) in key_column.iter().enumerate() {
                        if let Some(key) = key {
                            if key == &key_param {
                                rows.push(Some(i));
                                break;
                            }
                        }
                    }
                }
            }
            ColumnType::MYSQL_TYPE_SHORT => {
                for param in in_list_keys {
                    let param = format!("{}", param);
                    if param == "NULL" {
                        continue;
                    }
                    let key = param.parse::<u64>().unwrap();
                    let key_param = Value::UInt(key);
                    for (i, key) in key_column.iter().enumerate() {
                        if let Some(key) = key {
                            if key == &key_param {
                                rows.push(Some(i));
                                break;
                            }
                        }
                    }
                }
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_LONGLONG => {
                for param in in_list_keys {
                    let param = format!("{}", param);
                    if param == "NULL" {
                        continue;
                    }
                    let key = param.parse::<i64>().unwrap();
                    let key_param = Value::Int(key);
                    for (i, key) in key_column.iter().enumerate() {
                        if let Some(key) = key {
                            if key == &key_param {
                                rows.push(Some(i));
                                break;
                            }
                        }
                    }
                }
            }
            ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => {
                for param in in_list_keys {
                    let param = format!("{}", param);
                    if param == "NULL" {
                        continue;
                    }
                    let key = param.parse::<f64>().unwrap();
                    let key_param = Value::Double(key);
                    for (i, key) in key_column.iter().enumerate() {
                        if let Some(key) = key {
                            if key == &key_param {
                                rows.push(Some(i));
                                break;
                            }
                        }
                    }
                }
            }
            ColumnType::MYSQL_TYPE_VAR_STRING => {
                for param in in_list_keys {
                    let param = format!("{}", param);
                    if param == "NULL" {
                        continue;
                    }
                    let param_str = param
                        .strip_prefix('\'')
                        .and_then(|s| s.strip_suffix('\''))
                        .unwrap_or(&param);
                    let key = param_str.as_bytes().to_vec();
                    let key_param = Value::Bytes(key);
                    for (i, key) in key_column.iter().enumerate() {
                        if let Some(key) = key {
                            if key == &key_param {
                                rows.push(Some(i));
                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        // step-2: write columns based on the matched rows
        // return NULL if params not matched.
        if rows.is_empty() {
            return results.completed(0, 0);
        }

        let value_idx1 = value_idx_vec[0];
        let value_field1 = self.schema[value_idx1].clone();
        let value_column1 = self.block[value_idx1].clone();

        let value_idx2 = value_idx_vec[1];
        let value_field2 = self.schema[value_idx2].clone();
        let value_column2 = self.block[value_idx2].clone();

        let cols = vec![value_field1.clone(), value_field2.clone()];
        let mut rw = results.start(&cols)?;

        for row in rows.into_iter().map(|r| r.unwrap()) {
            let value1 = value_column1[row].clone();
            let value2 = value_column2[row].clone();
            for (value, value_field) in [
                (value1, value_field1.clone()),
                (value2, value_field2.clone()),
            ] {
                match value {
                    Some(val) => match val {
                        Value::Bytes(v) => {
                            rw.write_col(v)?;
                        }
                        Value::Int(v) => match value_field.coltype {
                            ColumnType::MYSQL_TYPE_TINY => {
                                rw.write_col(v as i8)?;
                            }
                            ColumnType::MYSQL_TYPE_SHORT => {
                                rw.write_col(v as u16)?;
                            }
                            ColumnType::MYSQL_TYPE_LONG => {
                                rw.write_col(v as i32)?;
                            }
                            ColumnType::MYSQL_TYPE_LONGLONG => {
                                rw.write_col(v)?;
                            }
                            _ => {
                                unreachable!()
                            }
                        },
                        Value::UInt(v) => match value_field.coltype {
                            ColumnType::MYSQL_TYPE_TINY => {
                                rw.write_col(v as u8)?;
                            }
                            ColumnType::MYSQL_TYPE_SHORT => {
                                rw.write_col(v as u16)?;
                            }
                            ColumnType::MYSQL_TYPE_LONG => {
                                rw.write_col(v as u32)?;
                            }
                            ColumnType::MYSQL_TYPE_LONGLONG => {
                                rw.write_col(v)?;
                            }
                            _ => {
                                unreachable!()
                            }
                        },
                        Value::Float(v) => {
                            rw.write_col(v)?;
                        }
                        Value::Double(v) => {
                            rw.write_col(v)?;
                        }
                        _ => {
                            rw.write_col("")?;
                        }
                    },
                    None => {
                        rw.write_col(None::<i64>)?;
                    }
                }
            }
            rw.end_row()?;
        }
        rw.finish()
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, _sql: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        results.completed(0, 0)
    }
}

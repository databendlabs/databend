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
    block: Vec<Vec<Value>>,

    prepared_id: u32,
    prepared: HashMap<u32, (usize, usize)>,
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

        let block = vec![
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)],
            vec![
                Value::Bytes("Alice".as_bytes().to_vec()),
                Value::Bytes("Bob".as_bytes().to_vec()),
                Value::Bytes("Lily".as_bytes().to_vec()),
                Value::Bytes("Tom".as_bytes().to_vec()),
            ],
            vec![
                Value::UInt(24),
                Value::UInt(35),
                Value::UInt(41),
                Value::UInt(55),
            ],
            vec![
                Value::Double(100.0),
                Value::Double(200.1),
                Value::Double(1000.20),
                Value::Double(3000.55),
            ],
            vec![Value::Int(1), Value::Int(0), Value::Int(1), Value::Int(0)],
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
        let mut value = None;

        // Only support simple SQL select one field with an equal filter.
        // for example: SELECT name FROM user WHERE id = 1;
        if asts.len() == 1 {
            if let Statement::Query(query) = &asts[0] {
                if let SetExpr::Select(select) = *query.body.clone() {
                    if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = &select.projection[0]
                    {
                        value = Some(ident.value.clone());
                    }
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        table = Some(name.0[0].value.clone());
                    }
                    if let Some(Expr::BinaryOp { left, op, .. }) = &select.selection {
                        if op == &BinaryOperator::Eq {
                            if let Expr::Identifier(ident) = *left.clone() {
                                key = Some(ident.value.clone());
                            }
                        }
                    }
                }
            }
        }

        self.prepared_id += 1;
        let prepared_id = self.prepared_id;

        if table.is_some() && key.is_some() && value.is_some() {
            let table = table.unwrap();
            let key = key.unwrap();
            let value = value.unwrap();

            let key_col = &self
                .schema
                .iter()
                .enumerate()
                .find(|&(_, f)| f.column == key);

            let value_col = &self
                .schema
                .iter()
                .enumerate()
                .find(|&(_, f)| f.column == value);

            if table == self.table && key_col.is_some() && value_col.is_some() {
                let (key_idx, key_col) = key_col.unwrap();
                let (value_idx, value_col) = value_col.unwrap();

                let prepared_idices = (key_idx, value_idx);
                self.prepared.insert(prepared_id, prepared_idices);

                // keys are bind as string type.
                let mut key_col = key_col.clone();
                key_col.coltype = ColumnType::MYSQL_TYPE_VAR_STRING;

                let key_cols = vec![key_col];
                let value_cols = vec![value_col.clone()];

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
        param_parser: msql_srv::ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let params: Vec<_> = param_parser
            .into_iter()
            .map(|p| p.value)
            .collect::<Vec<_>>();

        // ignore if params are empty.
        if params.len() != 1 {
            return results.completed(0, 0);
        }
        let param = params[0];

        let (key_idx, value_idx) = self.prepared.get(&id).unwrap();

        let key_field = self.schema[*key_idx].clone();
        let key_column = self.block[*key_idx].clone();

        let mut row = None;
        // find matched row by compare key params.
        match key_field.coltype {
            ColumnType::MYSQL_TYPE_TINY
            | ColumnType::MYSQL_TYPE_SHORT
            | ColumnType::MYSQL_TYPE_LONG
            | ColumnType::MYSQL_TYPE_LONGLONG => {
                let param: &str = param.into();
                let key = param.parse::<i64>().unwrap();
                let key_param = Value::Int(key);
                for (i, key) in key_column.iter().enumerate() {
                    if key == &key_param {
                        row = Some(i);
                        break;
                    }
                }
            }
            ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => {
                let param: &str = param.into();
                let key = param.parse::<f64>().unwrap();
                let key_param = Value::Double(key);
                for (i, key) in key_column.iter().enumerate() {
                    if key == &key_param {
                        row = Some(i);
                        break;
                    }
                }
            }
            ColumnType::MYSQL_TYPE_VAR_STRING => {
                let param: &str = param.into();
                let key = param.as_bytes().to_vec();
                let key_param = Value::Bytes(key);
                for (i, key) in key_column.iter().enumerate() {
                    if key == &key_param {
                        row = Some(i);
                        break;
                    }
                }
            }
            _ => {}
        }

        // return NULL if params not matched.
        if row.is_none() {
            return results.completed(0, 0);
        }
        let row = row.unwrap();

        let value_field = self.schema[*value_idx].clone();
        let value_column = self.block[*value_idx].clone();
        let value = value_column[row].clone();

        let cols = vec![value_field.clone()];

        let mut rw = results.start(&cols)?;
        match value {
            Value::Bytes(v) => {
                rw.write_col(v)?;
            }
            Value::Int(v) => match value_field.coltype {
                ColumnType::MYSQL_TYPE_TINY => {
                    rw.write_col(v as i8)?;
                }
                ColumnType::MYSQL_TYPE_SHORT => {
                    rw.write_col(v as i16)?;
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
        }
        rw.finish()
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, _sql: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        results.completed(0, 0)
    }
}

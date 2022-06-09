// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use databend_query::sql::statements::AlterTableAction;
use databend_query::sql::statements::DfAlterTable;
use databend_query::sql::statements::DfCreateTable;
use databend_query::sql::statements::DfDescribeTable;
use databend_query::sql::statements::DfDropTable;
use databend_query::sql::statements::DfQueryStatement;
use databend_query::sql::statements::DfRenameTable;
use databend_query::sql::statements::DfShowCreateTable;
use databend_query::sql::statements::DfTruncateTable;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn create_table() -> Result<()> {
    // positive case
    let sql = "CREATE TABLE t(c1 int) ENGINE = Fuse location = '/data/33.csv' ";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![make_column_def("c1", None, DataType::Int(None))],
        engine: "Fuse".to_string(),
        options: maplit::btreemap! {"location".into() => "/data/33.csv".into()},
        like: None,
        query: None,
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;

    let sql = "CREATE TABLE t(`c1` int) ENGINE = Fuse location = '/data/33.csv' ";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![make_column_def("c1", Some('`'), DataType::Int(None))],
        engine: "Fuse".to_string(),
        options: maplit::btreemap! {"location".into() => "/data/33.csv".into()},
        like: None,
        query: None,
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;

    let sql = "CREATE TABLE t('c1' int) ENGINE = Fuse location = '/data/33.csv' ";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![make_column_def("c1", Some('\''), DataType::Int(None))],
        engine: "Fuse".to_string(),
        options: maplit::btreemap! {"location".into() => "/data/33.csv".into()},
        like: None,
        query: None,
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;

    // positive case: it is ok for parquet files not to have columns specified
    let sql = "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Fuse location = 'foo.parquet' comment = 'foo'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("t")]),
        columns: vec![
            make_column_def("c1", None, DataType::Int(None)),
            make_column_def("c2", None, DataType::BigInt(None)),
            make_column_def("c3", None, DataType::Varchar(Some(255))),
        ],
        engine: "Fuse".to_string(),

        options: maplit::btreemap! {
            "location".into() => "foo.parquet".into(),
            "comment".into() => "foo".into(),
        },
        like: None,
        query: None,
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;

    // create table like statement
    let sql = "CREATE TABLE db1.test1 LIKE db2.test2 ENGINE = Parquet location = 'batcave'";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("db1"), Ident::new("test1")]),
        columns: vec![],
        engine: "Parquet".to_string(),

        options: maplit::btreemap! {"location".into() => "batcave".into()},
        like: Some(ObjectName(vec![Ident::new("db2"), Ident::new("test2")])),
        query: None,
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;

    // create table as select statement
    let sql = "CREATE TABLE db1.test1(c1 int, c2 varchar(255)) ENGINE = Parquet location = 'batcave' AS SELECT * FROM t2";
    let expected = DfStatement::CreateTable(DfCreateTable {
        if_not_exists: false,
        name: ObjectName(vec![Ident::new("db1"), Ident::new("test1")]),
        columns: vec![
            make_column_def("c1", None, DataType::Int(None)),
            make_column_def("c2", None, DataType::Varchar(Some(255))),
        ],
        engine: "Parquet".to_string(),

        options: maplit::btreemap! {"location".into() => "batcave".into()},
        like: None,
        query: Some(Box::new(DfQueryStatement {
            distinct: false,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident::new("t2")]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                    instant: None,
                },
                joins: vec![],
            }],
            projection: vec![SelectItem::Wildcard],
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            format: None,
        })),
        cluster_keys: vec![],
    });
    expect_parse_ok(sql, expected)?;
    Ok(())
}

#[test]
fn create_table_select() -> Result<()> {
    expect_parse_ok(
        "CREATE TABLE foo AS SELECT a, b FROM bar",
        DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("foo")]),
            columns: vec![],
            engine: "FUSE".to_string(),
            options: maplit::btreemap! {},
            like: None,
            query: Some(verified_query("SELECT a, b FROM bar")?),
            cluster_keys: vec![],
        }),
    )?;

    expect_parse_ok(
        "CREATE TABLE foo (a INT) SELECT a, b FROM bar",
        DfStatement::CreateTable(DfCreateTable {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("foo")]),
            columns: vec![make_column_def("a", None, DataType::Int(None))],
            engine: "FUSE".to_string(),
            options: maplit::btreemap! {},
            like: None,
            query: Some(verified_query("SELECT a, b FROM bar")?),
            cluster_keys: vec![],
        }),
    )?;

    Ok(())
}

#[test]
fn drop_table() -> Result<()> {
    {
        let sql = "DROP TABLE t1";
        let expected = DfStatement::DropTable(DfDropTable {
            if_exists: false,
            name: ObjectName(vec![Ident::new("t1")]),
            all: false,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "DROP TABLE IF EXISTS t1";
        let expected = DfStatement::DropTable(DfDropTable {
            if_exists: true,
            name: ObjectName(vec![Ident::new("t1")]),
            all: false,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "DROP TABLE t1 all";
        let expected = DfStatement::DropTable(DfDropTable {
            if_exists: false,
            name: ObjectName(vec![Ident::new("t1")]),
            all: true,
        });
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}

#[test]
fn alter_table() -> Result<()> {
    // alter table rename
    {
        let sql = "ALTER TABLE t1 RENAME TO t2";
        let table_name = ObjectName(vec![Ident::new("t1")]);
        let new_table_name = ObjectName(vec![Ident::new("t2")]);
        let expected = DfStatement::AlterTable(DfAlterTable {
            if_exists: false,
            table_name,
            action: AlterTableAction::RenameTable(new_table_name),
        });
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}

#[test]
fn rename_table() -> Result<()> {
    {
        let sql = "RENAME TABLE t1 TO t2";
        let mut name_map = HashMap::new();
        name_map.insert(
            ObjectName(vec![Ident::new("t1")]),
            ObjectName(vec![Ident::new("t2")]),
        );
        let expected = DfStatement::RenameTable(DfRenameTable { name_map });
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}

#[test]
fn describe_table() -> Result<()> {
    {
        let sql = "DESCRIBE t1";
        let expected = DfStatement::DescribeTable(DfDescribeTable {
            name: ObjectName(vec![Ident::new("t1")]),
        });
        expect_parse_ok(sql, expected)?;
    }
    {
        let sql = "DESC t1";
        let expected = DfStatement::DescribeTable(DfDescribeTable {
            name: ObjectName(vec![Ident::new("t1")]),
        });
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}

#[test]
fn show_create_table_test() -> Result<()> {
    expect_parse_ok(
        "SHOW CREATE TABLE test",
        DfStatement::ShowCreateTable(DfShowCreateTable {
            name: ObjectName(vec![Ident::new("test")]),
        }),
    )?;

    Ok(())
}

#[test]
fn truncate_table() -> Result<()> {
    {
        let sql = "TRUNCATE TABLE t1";
        let expected = DfStatement::TruncateTable(DfTruncateTable {
            name: ObjectName(vec![Ident::new("t1")]),
            purge: false,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "TRUNCATE TABLE t1 purge";
        let expected = DfStatement::TruncateTable(DfTruncateTable {
            name: ObjectName(vec![Ident::new("t1")]),
            purge: true,
        });
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}

#[test]
fn alter_cluster_key() -> Result<()> {
    {
        let sql = "ALTER TABLE t1 CLUSTER BY (a, b)";
        let expected = DfStatement::AlterTable(DfAlterTable {
            if_exists: false,
            table_name: ObjectName(vec![Ident::new("t1")]),
            action: AlterTableAction::AlterClusterKey(vec![
                Expr::Identifier(Ident::new("a")),
                Expr::Identifier(Ident::new("b")),
            ]),
        });
        expect_parse_ok(sql, expected)?;
    }
    Ok(())
}

#[test]
fn drop_cluster_key() -> Result<()> {
    {
        let sql = "ALTER TABLE t1 DROP CLUSTER KEY";
        let expected = DfStatement::AlterTable(DfAlterTable {
            if_exists: false,
            table_name: ObjectName(vec![Ident::new("t1")]),
            action: AlterTableAction::DropClusterKey,
        });
        expect_parse_ok(sql, expected)?;
    }
    Ok(())
}

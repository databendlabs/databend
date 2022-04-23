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

use nom::branch::alt;
use nom::combinator::map;
use nom::combinator::value;

use crate::ast::*;
use crate::parser::expr::*;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

pub fn statement(i: Input) -> IResult<Statement> {
    let query = map(query, |query| Statement::Select(Box::new(query)));
    let show_tables = value(Statement::ShowTables, rule! { SHOW ~ TABLES });
    let show_databases = value(Statement::ShowDatabases, rule! { SHOW ~ DATABASES });
    let show_settings = value(Statement::ShowSettings, rule! { SHOW ~ SETTINGS });
    let show_process_list = value(Statement::ShowProcessList, rule! { SHOW ~ PROCESSLIST });
    let show_create_table = map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, _, database, table)| Statement::ShowCreateTable {
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let explain = map(
        rule! {
            EXPLAIN ~ ANALYZE? ~ #statement
        },
        |(_, analyze, statement)| Statement::Explain {
            analyze: analyze.is_some(),
            query: Box::new(statement),
        },
    );
    let describe = map(
        rule! {
            DESCRIBE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, database, table)| Statement::Describe {
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let create_table = map(
        rule! {
            CREATE ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ "(" ~ #comma_separated_list1(column_def) ~ ")"
        },
        |(_, _, if_not_exists, database, table, _, columns, _)| Statement::CreateTable {
            if_not_exists: if_not_exists.is_some(),
            database: database.map(|(name, _)| name),
            table,
            columns,
            engine: "".to_string(),
            options: vec![],
            like_db: None,
            like_table: None,
        },
    );
    let create_table_like = map(
        rule! {
            CREATE ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ LIKE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, if_not_exists, database, table, _, like_db, like_table)| Statement::CreateTable {
            if_not_exists: if_not_exists.is_some(),
            database: database.map(|(name, _)| name),
            table,
            columns: vec![],
            engine: "".to_string(),
            options: vec![],
            like_db: like_db.map(|(like_db, _)| like_db),
            like_table: Some(like_table),
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, if_exists, database, table)| Statement::DropTable {
            if_exists: if_exists.is_some(),
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, database, table)| Statement::TruncateTable {
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let create_database = map(
        rule! {
            CREATE ~ DATABASE ~ ( IF ~ NOT ~ EXISTS )? ~ #ident
        },
        |(_, _, if_not_exists, name)| Statement::CreateDatabase {
            if_not_exists: if_not_exists.is_some(),
            name,
            engine: "".to_string(),
            options: vec![],
        },
    );
    let use_database = map(
        rule! {
            USE ~ #ident
        },
        |(_, name)| Statement::UseDatabase { name },
    );
    let kill = map(
        rule! {
            KILL ~ #ident
        },
        |(_, object_id)| Statement::KillStmt { object_id },
    );

    rule!(
        #query
        | #show_tables : "`SHOW TABLES`"
        | #show_databases : "`SHOW DATABASES`"
        | #show_settings : "`SHOW SETTINGS`"
        | #show_process_list : "`SHOW PROCESSLIST`"
        | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
        | #explain : "`EXPLAIN [ANALYZE] <statement>`"
        | #describe : "`DESCRIBE [<database>.]<table>`"
        | #create_table : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> (<column definition>, ...)`"
        | #create_table_like : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> LIKE [<database>.]<table>`"
        | #drop_table : "`DROP TABLE [IF EXIST] [<database>.]<table>`"
        | #truncate_table : "`TRUNCATE TABLE [<database>.]<table>`"
        | #create_database : "`CREATE DATABASE [IF NOT EXIST] <database>`"
        | #use_database : "`USE <database>`"
        | #kill : "`KILL <object id>`"
    )(i)
}

pub fn column_def(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultValue(Literal),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ NULL }),
    ));
    let default_value = map(rule! { DEFAULT ~ #literal }, |(_, default_value)| {
        ColumnConstraint::DefaultValue(default_value)
    });

    map(
        rule! {
            #ident ~ #type_name ~ ( #nullable | #default_value )* : "`<column name> <type> [NOT NULL | NULL] [DEFAULT <default value>]`"
        },
        |(name, data_type, constraints)| {
            let mut def = ColumnDefinition {
                name,
                data_type,
                nullable: false,
                default_value: None,
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::Nullable(nullable) => def.nullable = nullable,
                    ColumnConstraint::DefaultValue(default_value) => {
                        def.default_value = Some(default_value)
                    }
                }
            }
            def
        },
    )(i)
}

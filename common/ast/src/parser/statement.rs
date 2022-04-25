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

pub fn statements(i: Input) -> IResult<Vec<Statement>> {
    let stmt = map(
        rule! {
            #statement ~ ";"
        },
        |(stmt, _)| stmt,
    );
    rule!(
        #stmt+
    )(i)
}

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
        |(_, _, _, opt_database, table)| Statement::ShowCreateTable {
            database: opt_database.map(|(name, _)| name),
            table,
        },
    );
    let explain = map(
        rule! {
            EXPLAIN ~ ANALYZE? ~ #statement
        },
        |(_, opt_analyze, statement)| Statement::Explain {
            analyze: opt_analyze.is_some(),
            query: Box::new(statement),
        },
    );
    let describe = map(
        rule! {
            DESCRIBE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, opt_database, table)| Statement::Describe {
            database: opt_database.map(|(name, _)| name),
            table,
        },
    );
    let create_table = map(
        rule! {
            CREATE ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ "(" ~ #comma_separated_list1(column_def) ~ ")"
        },
        |(_, _, opt_if_not_exists, opt_database, table, _, columns, _)| Statement::CreateTable {
            if_not_exists: opt_if_not_exists.is_some(),
            database: opt_database.map(|(name, _)| name),
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
        |(_, _, opt_if_not_exists, opt_database, table, _, opt_like_db, like_table)| {
            Statement::CreateTable {
                if_not_exists: opt_if_not_exists.is_some(),
                database: opt_database.map(|(name, _)| name),
                table,
                columns: vec![],
                engine: "".to_string(),
                options: vec![],
                like_db: opt_like_db.map(|(like_db, _)| like_db),
                like_table: Some(like_table),
            }
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_if_exists, opt_database, table)| Statement::DropTable {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(name, _)| name),
            table,
        },
    );
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_database, table)| Statement::TruncateTable {
            database: opt_database.map(|(name, _)| name),
            table,
        },
    );
    let create_database = map(
        rule! {
            CREATE ~ DATABASE ~ ( IF ~ NOT ~ EXISTS )? ~ #ident
        },
        |(_, _, opt_if_not_exists, name)| Statement::CreateDatabase {
            if_not_exists: opt_if_not_exists.is_some(),
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
    let insert = map(
        rule! {
            INSERT ~ INTO ~ TABLE? ~ #ident ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )? ~ #insert_source
        },
        |(_, _, _, table, opt_columns, source)| Statement::Insert {
            table,
            columns: opt_columns
                .map(|(_, columns, _)| columns)
                .unwrap_or_default(),
            source,
        },
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
        | #insert : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
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

pub fn insert_source(i: Input) -> IResult<InsertSource> {
    let streaming = map(
        rule! {
            FORMAT ~ #ident
        },
        |(_, format)| InsertSource::Streaming {
            format: format.name,
        },
    );
    let values = map(
        rule! {
            VALUES ~ #values_tokens
        },
        |(_, values_tokens)| InsertSource::Values { values_tokens },
    );
    let query = map(query, |query| InsertSource::Select {
        query: Box::new(query),
    });

    rule!(
        #streaming
        | #values
        | #query
    )(i)
}

pub fn values_tokens(i: Input) -> IResult<Input> {
    let semicolon_pos = i
        .iter()
        .position(|token| token.text() == ";")
        .unwrap_or(i.len() - 1);
    let (value_tokens, rest_tokens) = i.split_at(semicolon_pos);
    Ok((rest_tokens, value_tokens))
}

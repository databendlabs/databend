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
    let stmt = map(statement, Some);
    let eoi = map(rule! { &EOI }, |_| None);

    map(
        rule!(
            #separated_list1(rule! { ";"+ }, rule! { #stmt | #eoi })
        ),
        |stmts| stmts.into_iter().flatten().collect(),
    )(i)
}

pub fn statement(i: Input) -> IResult<Statement> {
    let explain = map(
        rule! {
            EXPLAIN ~ ( PIPELINE | GRAPH )? ~ #statement
        },
        |(_, opt_kind, statement)| Statement::Explain {
            kind: match opt_kind.map(|token| token.kind) {
                Some(TokenKind::PIPELINE) => ExplainKind::Pipeline,
                Some(TokenKind::GRAPH) => ExplainKind::Graph,
                None => ExplainKind::Syntax,
                _ => unreachable!(),
            },
            query: Box::new(statement),
        },
    );
    let show_databases = map(
        rule! {
            SHOW ~ DATABASES ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowDatabases { limit },
    );
    let show_create_database = map(
        rule! {
            SHOW ~ CREATE ~ DATABASE ~ #ident
        },
        |(_, _, _, database)| Statement::ShowCreateDatabase { database },
    );
    let create_database = map(
        rule! {
            CREATE ~ DATABASE ~ ( IF ~ NOT ~ EXISTS )? ~ #ident ~ #engine?
        },
        |(_, _, opt_if_not_exists, database, opt_engine)| Statement::CreateDatabase {
            if_not_exists: opt_if_not_exists.is_some(),
            database,
            engine: opt_engine.unwrap_or(Engine::Null),
            options: vec![],
        },
    );
    let drop_database = map(
        rule! {
            DROP ~ DATABASE ~ ( IF ~ EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, database)| Statement::DropDatabase {
            if_exists: opt_if_exists.is_some(),
            database,
        },
    );
    let use_database = map(
        rule! {
            USE ~ #ident
        },
        |(_, database)| Statement::UseDatabase { database },
    );
    let show_tables = map(
        rule! {
            SHOW ~ FULL? ~ TABLES ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_database, limit)| Statement::ShowTables {
            database: opt_database.map(|(_, database)| database),
            full: opt_full.is_some(),
            limit,
        },
    );
    let show_create_table = map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, _, opt_database, table)| Statement::ShowCreateTable {
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );
    let show_tables_status = map(
        rule! {
            SHOW ~ TABLE ~ STATUS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| Statement::ShowTablesStatus {
            database: opt_database.map(|(_, database)| database),
            limit,
        },
    );
    let create_table = map(
        rule! {
            CREATE ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident
            ~ #create_table_source
            ~ #engine?
            ~ ( CLUSTER ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" )?
        },
        |(_, _, opt_if_not_exists, opt_database, table, source, opt_engine, opt_cluster_by)| {
            Statement::CreateTable {
                if_not_exists: opt_if_not_exists.is_some(),
                database: opt_database.map(|(name, _)| name),
                table,
                source,
                engine: opt_engine.unwrap_or(Engine::Null),
                cluster_by: opt_cluster_by
                    .map(|(_, _, _, exprs, _)| exprs)
                    .unwrap_or_default(),
                options: vec![],
            }
        },
    );
    let describe = map(
        rule! {
            DESCRIBE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, opt_database, table)| Statement::Describe {
            database: opt_database.map(|(database, _)| database),
            table,
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
    let alter_table = map(
        rule! {
            ALTER ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ #alter_table_action
        },
        |(_, _, opt_if_exists, opt_database, table, action)| Statement::AlterTable {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(name, _)| name),
            table,
            action,
        },
    );
    let rename_table = map(
        rule! {
            RENAME ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ TO ~ #ident
        },
        |(_, _, opt_database, table, _, new_table)| Statement::RenameTable {
            database: opt_database.map(|(name, _)| name),
            table,
            new_table,
        },
    );
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ PURGE?
        },
        |(_, _, opt_database, table, opt_purge)| Statement::TruncateTable {
            database: opt_database.map(|(name, _)| name),
            table,
            purge: opt_purge.is_some(),
        },
    );
    let optimize_table = map(
        rule! {
            OPTIMIZE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ #optimize_table_action
        },
        |(_, _, opt_database, table, action)| Statement::OptimizeTable {
            database: opt_database.map(|(name, _)| name),
            table,
            action,
        },
    );
    let create_view = map(
        rule! {
            CREATE ~ VIEW ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #ident ~ "." )? ~ #ident ~ AS ~ #query
        },
        |(_, _, opt_if_not_exists, opt_database, view, _, query)| Statement::CreateView {
            if_not_exists: opt_if_not_exists.is_some(),
            database: opt_database.map(|(name, _)| name),
            view,
            query: Box::new(query),
        },
    );
    let drop_view = map(
        rule! {
            DROP ~ VIEW ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, _, opt_if_exists, opt_database, view)| Statement::DropView {
            if_exists: opt_if_exists.is_some(),
            database: opt_database.map(|(name, _)| name),
            view,
        },
    );
    let show_settings = value(Statement::ShowSettings, rule! { SHOW ~ SETTINGS });
    let show_process_list = value(Statement::ShowProcessList, rule! { SHOW ~ PROCESSLIST });
    let show_metrics = value(Statement::ShowMetrics, rule! { SHOW ~ METRICS });
    let show_functions = map(
        rule! {
            SHOW ~ FUNCTIONS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowFunctions { limit },
    );
    let kill_stmt = map(
        rule! {
            KILL ~ #kill_target ~ #ident
        },
        |(_, kill_target, object_id)| Statement::KillStmt {
            kill_target,
            object_id,
        },
    );
    let set_variable = map(
        rule! {
            SET ~ #ident ~ "=" ~ #literal
        },
        |(_, variable, _, value)| Statement::SetVariable { variable, value },
    );
    let insert = map(
        rule! {
            INSERT ~ ( INTO | OVERWRITE ) ~ TABLE?
            ~ ( #ident ~ "." )? ~ #ident
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ #insert_source
        },
        |(_, overwrite, _, opt_database, table, opt_columns, source)| Statement::Insert {
            database: opt_database.map(|(database, _)| database),
            table,
            columns: opt_columns
                .map(|(_, columns, _)| columns)
                .unwrap_or_default(),
            source,
            overwrite: overwrite.kind == OVERWRITE,
        },
    );
    let create_udf = map(
        rule! {
            CREATE ~ FUNCTION ~ ( IF ~ NOT ~ EXISTS )?
            ~ #ident
            ~ AS ~ "(" ~ #comma_separated_list0(ident) ~ ")"
            ~ "->" ~ #expr
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            udf_name,
            _,
            _,
            parameters,
            _,
            _,
            definition,
            opt_description,
        )| {
            Statement::CreateUDF {
                if_not_exists: opt_if_not_exists.is_some(),
                udf_name,
                parameters,
                definition: Box::new(definition),
                description: opt_description.map(|(_, _, description)| description),
            }
        },
    );
    let drop_udf = map(
        rule! {
            DROP ~ FUNCTION ~ ( IF ~ EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, udf_name)| Statement::DropUDF {
            if_exists: opt_if_exists.is_some(),
            udf_name,
        },
    );
    let alter_udf = map(
        rule! {
            ALTER ~ FUNCTION
            ~ #ident
            ~ AS ~ "(" ~ #comma_separated_list0(ident) ~ ")"
            ~ "->" ~ #expr
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, udf_name, _, _, parameters, _, _, definition, opt_description)| {
            Statement::AlterUDF {
                udf_name,
                parameters,
                definition: Box::new(definition),
                description: opt_description.map(|(_, _, description)| description),
            }
        },
    );

    alt((
        rule!(
            #explain : "`EXPLAIN [ANALYZE] <statement>`"
            | #map(query, |query| Statement::Query(Box::new(query)))
            | #show_databases : "`SHOW DATABASES [<show limit>]`"
            | #show_create_database : "`SHOW CREATE DATABASE <database>`"
            | #create_database : "`CREATE DATABASE [IF NOT EXIST] <database> [ENGINE = <engine>]`"
            | #drop_database : "`DROP DATABASE [IF EXISTS] <database>`"
            | #use_database : "`USE <database>`"
            | #show_tables : "`SHOW [FULL] TABLES [FROM <database>] [<show limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show limit>]`"
            | #create_table : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> <source> [ENGINE = <engine>]`"
            | #describe : "`DESCRIBE [<database>.]<table>`"
            | #drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
            | #alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #truncate_table : "`TRUNCATE TABLE [<database>.]<table> [PURGE]`"
            | #optimize_table : "`OPTIMIZE TABLE [<database>.]<table> (ALL | PURGE | COMPACT)`"
        ),
        rule!(
            #create_view : "`CREATE VIEW [IF NOT EXISTS] [<database>.]<view> AS SELECT ...`"
            | #drop_view : "`DROP VIEW [IF EXISTS] [<database>.]<view>`"
            | #show_settings : "`SHOW SETTINGS`"
            | #show_process_list : "`SHOW PROCESSLIST`"
            | #show_metrics : "`SHOW METRICS`"
            | #show_functions : "`SHOW FUNCTIONS [<show limit>]`"
            | #kill_stmt : "`KILL (QUERY | CONNECTION) <object_id>`"
            | #set_variable : "`SET <variable> = <value>`"
            | #insert : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #create_udf : "`CREATE FUNCTION [IF NOT EXISTS] <udf_name> (<parameter>, ...) -> <definition expr> [DESC = <description>]`"
            | #drop_udf : "`DROP FUNCTION [IF EXISTS] <udf_name>`"
            | #alter_udf : "`ALTER FUNCTION <udf_name> (<parameter>, ...) -> <definition expr> [DESC = <description>]`"
        ),
    ))(i)
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
            ( #ident | #lit_string_ident )
            ~ #type_name
            ~ ( #nullable | #default_value )*
            : "`<column name> <type> [NOT NULL | NULL] [DEFAULT <default value>]`"
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

pub fn create_table_source(i: Input) -> IResult<CreateTableSource> {
    let columns = map(
        rule! {
            "(" ~ #comma_separated_list1(column_def) ~ ")"
        },
        |(_, columns, _)| CreateTableSource::Columns(columns),
    );
    let query = map(query, |query| CreateTableSource::Query(Box::new(query)));
    let like = map(
        rule! {
            LIKE ~ ( #ident ~ "." )? ~ #ident
        },
        |(_, opt_database, table)| CreateTableSource::Like {
            database: opt_database.map(|(database, _)| database),
            table,
        },
    );

    rule!(
        #columns
        | #query
        | #like
    )(i)
}

pub fn alter_table_action(i: Input) -> IResult<AlterTableAction> {
    let mut rename_table = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_table)| AlterTableAction::RenameTable { new_table },
    );

    rule!(
        #rename_table
    )(i)
}

pub fn optimize_table_action(i: Input) -> IResult<OptimizeTableAction> {
    alt((
        value(OptimizeTableAction::All, rule! { ALL }),
        value(OptimizeTableAction::Purge, rule! { PURGE }),
        value(OptimizeTableAction::Compact, rule! { COMPACT }),
    ))(i)
}

pub fn kill_target(i: Input) -> IResult<KillTarget> {
    alt((
        value(KillTarget::Query, rule! { QUERY }),
        value(KillTarget::Connection, rule! { CONNECTION }),
    ))(i)
}

pub fn show_limit(i: Input) -> IResult<ShowLimit> {
    let limit_like = map(
        rule! {
            LIKE ~ #literal_string
        },
        |(_, pattern)| ShowLimit::Like { pattern },
    );
    let limit_where = map(
        rule! {
            WHERE ~ #expr
        },
        |(_, selection)| ShowLimit::Where {
            selection: Box::new(selection),
        },
    );

    rule!(
        #limit_like
        | #limit_where
    )(i)
}

pub fn engine(i: Input) -> IResult<Engine> {
    let engine = alt((
        value(Engine::Null, rule! { NULL }),
        value(Engine::Memory, rule! { MEMORY }),
        value(Engine::Fuse, rule! { FUSE }),
        value(Engine::Github, rule! { GITHUB }),
        value(Engine::View, rule! { VIEW }),
    ));

    map(
        rule! {
            ENGINE ~ "=" ~ #engine
        },
        |(_, _, engine)| engine,
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

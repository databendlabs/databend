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

use std::collections::BTreeMap;
use std::time::Duration;

use common_meta_app::principal::AuthType;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::PrincipalIdentity;
use common_meta_app::principal::UserIdentity;
use common_meta_app::principal::UserPrivilegeType;
use common_meta_app::schema::CatalogType;
use common_meta_app::share::ShareGrantObjectName;
use common_meta_app::share::ShareGrantObjectPrivilege;
use common_meta_app::share::ShareNameIdent;
use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::Slice;

use crate::ast::*;
use crate::input::Input;
use crate::parser::copy::copy_into;
use crate::parser::data_mask::data_mask_policy;
use crate::parser::expr::subexpr;
use crate::parser::expr::*;
use crate::parser::query::*;
use crate::parser::share::share_endpoint_uri_location;
use crate::parser::stage::*;
use crate::parser::token::*;
use crate::rule;
use crate::util::*;
use crate::Error;
use crate::ErrorKind;

pub enum ShowGrantOption {
    PrincipalIdentity(PrincipalIdentity),
    ShareGrantObjectName(ShareGrantObjectName),
    ShareName(String),
}

#[derive(Clone)]
pub enum CreateDatabaseOption {
    DatabaseEngine(DatabaseEngine),
    FromShare(ShareNameIdent),
}

pub fn statement(i: Input) -> IResult<StatementMsg> {
    let explain = map_res(
        rule! {
            EXPLAIN ~ ( AST | SYNTAX | PIPELINE | JOIN | GRAPH | FRAGMENTS | RAW | MEMO )? ~ #statement
        },
        |(_, opt_kind, statement)| {
            Ok(Statement::Explain {
                kind: match opt_kind.map(|token| token.kind) {
                    Some(TokenKind::AST) => {
                        let formatted_stmt = format_statement(statement.stmt.clone())
                            .map_err(|_| ErrorKind::Other("invalid statement"))?;
                        ExplainKind::Ast(formatted_stmt)
                    }
                    Some(TokenKind::SYNTAX) => {
                        let pretty_stmt = pretty_statement(statement.stmt.clone(), 10)
                            .map_err(|_| ErrorKind::Other("invalid statement"))?;
                        ExplainKind::Syntax(pretty_stmt)
                    }
                    Some(TokenKind::PIPELINE) => ExplainKind::Pipeline,
                    Some(TokenKind::JOIN) => ExplainKind::JOIN,
                    Some(TokenKind::GRAPH) => ExplainKind::Graph,
                    Some(TokenKind::FRAGMENTS) => ExplainKind::Fragments,
                    Some(TokenKind::RAW) => ExplainKind::Raw,
                    Some(TokenKind::MEMO) => ExplainKind::Memo("".to_string()),
                    None => ExplainKind::Plan,
                    _ => unreachable!(),
                },
                query: Box::new(statement.stmt),
            })
        },
    );
    let explain_analyze = map(
        rule! {
            EXPLAIN ~ ANALYZE ~ #statement
        },
        |(_, _, statement)| Statement::ExplainAnalyze {
            query: Box::new(statement.stmt),
        },
    );

    let create_task = map(
        rule! {
            CREATE ~ TASK ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #task_warehouse_option
            ~ SCHEDULE ~ "=" ~ #task_schedule_option
            ~ (SUSPEND_TASK_AFTER_NUM_FAILURES ~ "=" ~ #literal_u64)?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
            ~ AS ~ #statement
        },
        |(
            _,
            _,
            opt_if_not_exists,
            task,
            warehouse_opts,
            _,
            _,
            schedule_opts,
            suspend_opt,
            comment_opt,
            _,
            sql,
        )| {
            let sql = pretty_statement(sql.stmt, 10)
                .map_err(|_| ErrorKind::Other("invalid statement"))
                .unwrap();
            Statement::CreateTask(CreateTaskStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: task.to_string(),
                warehouse_opts,
                schedule_opts,
                suspend_task_after_num_failures: suspend_opt.map(|(_, _, num)| num),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
                sql,
            })
        },
    );

    let insert = map(
        rule! {
            INSERT ~ #hint? ~ ( INTO | OVERWRITE ) ~ TABLE?
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ #insert_source
        },
        |(_, opt_hints, overwrite, _, (catalog, database, table), opt_columns, source)| {
            Statement::Insert(InsertStmt {
                hints: opt_hints,
                catalog,
                database,
                table,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                source,
                overwrite: overwrite.kind == OVERWRITE,
            })
        },
    );

    let replace = map(
        rule! {
            REPLACE ~ #hint? ~ INTO?
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ (ON ~ CONFLICT? ~ "(" ~ #comma_separated_list1(ident) ~ ")")
            ~ (DELETE ~ WHEN ~ ^#expr)?
            ~ #insert_source
        },
        |(
            _,
            opt_hints,
            _,
            (catalog, database, table),
            opt_columns,
            (_, _, _, on_conflict_columns, _),
            opt_delete_when,
            source,
        )| {
            Statement::Replace(ReplaceStmt {
                hints: opt_hints,
                catalog,
                database,
                table,
                on_conflict_columns,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                source,
                delete_when: opt_delete_when.map(|(_, _, expr)| expr),
            })
        },
    );

    let merge = map(
        rule! {
            MERGE ~ #hint? ~ INTO ~ #dot_separated_idents_1_to_3 ~ #table_alias? ~ USING
            ~ #merge_source ~ ON ~ #expr ~ (#match_clause | #unmatch_clause)*
        },
        |(
            _,
            opt_hints,
            _,
            (catalog, database, table),
            target_alias,
            _,
            source,
            _,
            join_expr,
            merge_options,
        )| {
            Statement::MergeInto(MergeIntoStmt {
                hints: opt_hints,
                catalog,
                database,
                table_ident: table,
                source,
                target_alias,
                join_expr,
                merge_options,
            })
        },
    );

    let delete = map(
        rule! {
            DELETE ~ #hint? ~ FROM ~ #table_reference_only
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, opt_hints, _, table_reference, opt_selection)| Statement::Delete {
            hints: opt_hints,
            table_reference,
            selection: opt_selection.map(|(_, selection)| selection),
        },
    );

    let update = map(
        rule! {
            UPDATE ~ #hint? ~ #table_reference_only
            ~ SET ~ ^#comma_separated_list1(update_expr)
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, opt_hints, table, _, update_list, opt_selection)| {
            Statement::Update(UpdateStmt {
                hints: opt_hints,
                table,
                update_list,
                selection: opt_selection.map(|(_, selection)| selection),
            })
        },
    );

    let show_settings = map(
        rule! {
            SHOW ~ SETTINGS ~ (LIKE ~ #literal_string)?
        },
        |(_, _, opt_like)| Statement::ShowSettings {
            like: opt_like.map(|(_, like)| like),
        },
    );
    let show_stages = value(Statement::ShowStages, rule! { SHOW ~ STAGES });
    let show_process_list = value(Statement::ShowProcessList, rule! { SHOW ~ PROCESSLIST });
    let show_metrics = value(Statement::ShowMetrics, rule! { SHOW ~ METRICS });
    let show_engines = value(Statement::ShowEngines, rule! { SHOW ~ ENGINES });
    let show_functions = map(
        rule! {
            SHOW ~ FUNCTIONS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowFunctions { limit },
    );
    let show_table_functions = map(
        rule! {
            SHOW ~ TABLE_FUNCTIONS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowTableFunctions { limit },
    );
    let show_indexes = value(Statement::ShowIndexes, rule! { SHOW ~ INDEXES });

    // kill query 199;
    let kill_stmt = map(
        rule! {
            KILL ~ #kill_target ~ #parameter_to_string
        },
        |(_, kill_target, object_id)| Statement::KillStmt {
            kill_target,
            object_id,
        },
    );

    let set_variable = map(
        rule! {
            SET ~ GLOBAL? ~ #ident ~ "=" ~ #subexpr(0)
        },
        |(_, opt_is_global, variable, _, value)| Statement::SetVariable {
            is_global: opt_is_global.is_some(),
            variable,
            value: Box::new(value),
        },
    );

    let unset_variable = map(
        rule! {
            UNSET ~ #unset_source
        },
        |(_, unset_source)| {
            Statement::UnSetVariable(UnSetStmt {
                source: unset_source,
            })
        },
    );

    let set_role = map(
        rule! {
            SET ~ DEFAULT? ~ ROLE ~ #role_name
        },
        |(_, opt_is_default, _, role_name)| Statement::SetRole {
            is_default: opt_is_default.is_some(),
            role_name,
        },
    );

    // catalogs
    let show_catalogs = map(
        rule! {
            SHOW ~ CATALOGS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowCatalogs(ShowCatalogsStmt { limit }),
    );
    let show_create_catalog = map(
        rule! {
            SHOW ~ CREATE ~ CATALOG ~ #ident
        },
        |(_, _, _, catalog)| Statement::ShowCreateCatalog(ShowCreateCatalogStmt { catalog }),
    );
    // TODO: support `COMMENT` in create catalog
    // TODO: use a more specific option struct instead of BTreeMap
    let create_catalog = map(
        rule! {
            CREATE ~ CATALOG ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ TYPE ~ "=" ~ #catalog_type
            ~ CONNECTION ~ "=" ~ #connection_options
        },
        |(_, _, opt_if_not_exists, catalog, _, _, ty, _, _, options)| {
            Statement::CreateCatalog(CreateCatalogStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                catalog_name: catalog.to_string(),
                catalog_type: ty,
                catalog_options: options,
            })
        },
    );
    let drop_catalog = map(
        rule! {
            DROP ~ CATALOG ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, catalog)| {
            Statement::DropCatalog(DropCatalogStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
            })
        },
    );

    let show_databases = map(
        rule! {
            SHOW ~ FULL? ~ ( DATABASES | SCHEMAS ) ~ ( ( FROM | IN ) ~ ^#ident )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_catalog, limit)| {
            Statement::ShowDatabases(ShowDatabasesStmt {
                catalog: opt_catalog.map(|(_, catalog)| catalog),
                full: opt_full.is_some(),
                limit,
            })
        },
    );
    let show_create_database = map(
        rule! {
            SHOW ~ CREATE ~ ( DATABASE | SCHEMA ) ~ #dot_separated_idents_1_to_2
        },
        |(_, _, _, (catalog, database))| {
            Statement::ShowCreateDatabase(ShowCreateDatabaseStmt { catalog, database })
        },
    );
    let create_database = map(
        rule! {
            CREATE ~ ( DATABASE | SCHEMA ) ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #dot_separated_idents_1_to_2 ~ #create_database_option?
        },
        |(_, _, opt_if_not_exists, (catalog, database), create_database_option)| {
            match create_database_option {
                Some(CreateDatabaseOption::DatabaseEngine(engine)) => {
                    Statement::CreateDatabase(CreateDatabaseStmt {
                        if_not_exists: opt_if_not_exists.is_some(),
                        catalog,
                        database,
                        engine: Some(engine),
                        options: vec![],
                        from_share: None,
                    })
                }
                Some(CreateDatabaseOption::FromShare(share_name)) => {
                    Statement::CreateDatabase(CreateDatabaseStmt {
                        if_not_exists: opt_if_not_exists.is_some(),
                        catalog,
                        database,
                        engine: None,
                        options: vec![],
                        from_share: Some(share_name),
                    })
                }
                None => Statement::CreateDatabase(CreateDatabaseStmt {
                    if_not_exists: opt_if_not_exists.is_some(),
                    catalog,
                    database,
                    engine: None,
                    options: vec![],
                    from_share: None,
                }),
            }
        },
    );
    let drop_database = map(
        rule! {
            DROP ~ ( DATABASE | SCHEMA ) ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_2
        },
        |(_, _, opt_if_exists, (catalog, database))| {
            Statement::DropDatabase(DropDatabaseStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
            })
        },
    );

    let undrop_database = map(
        rule! {
            UNDROP ~ DATABASE ~ #dot_separated_idents_1_to_2
        },
        |(_, _, (catalog, database))| {
            Statement::UndropDatabase(UndropDatabaseStmt { catalog, database })
        },
    );

    let alter_database = map(
        rule! {
            ALTER ~ DATABASE ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_2 ~ #alter_database_action
        },
        |(_, _, opt_if_exists, (catalog, database), action)| {
            Statement::AlterDatabase(AlterDatabaseStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                action,
            })
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
            SHOW ~ FULL? ~ TABLES ~ HISTORY? ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_history, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowTables(ShowTablesStmt {
                catalog,
                database,
                full: opt_full.is_some(),
                limit,
                with_history: opt_history.is_some(),
            })
        },
    );
    let show_columns = map(
        rule! {
            SHOW ~ FULL? ~ COLUMNS ~ ( FROM | IN ) ~ #ident ~ (( FROM | IN ) ~ ^#dot_separated_idents_1_to_2)? ~ #show_limit?
        },
        |(_, opt_full, _, _, table, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowColumns(ShowColumnsStmt {
                catalog,
                database,
                table,
                full: opt_full.is_some(),
                limit,
            })
        },
    );
    let show_create_table = map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, (catalog, database, table))| {
            Statement::ShowCreateTable(ShowCreateTableStmt {
                catalog,
                database,
                table,
            })
        },
    );
    let describe_table = map(
        rule! {
            ( DESC | DESCRIBE ) ~ #dot_separated_idents_1_to_3
        },
        |(_, (catalog, database, table))| {
            Statement::DescribeTable(DescribeTableStmt {
                catalog,
                database,
                table,
            })
        },
    );

    // parse `show fields from` statement
    let show_fields = map(
        rule! {
            SHOW ~ FIELDS ~ FROM ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, (catalog, database, table))| {
            Statement::DescribeTable(DescribeTableStmt {
                catalog,
                database,
                table,
            })
        },
    );

    let show_tables_status = map(
        rule! {
            SHOW ~ ( TABLES | TABLE ) ~ STATUS ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowTablesStatus(ShowTablesStatusStmt {
                database: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    );
    let show_drop_tables_status = map(
        rule! {
            SHOW ~ DROP ~ ( TABLES | TABLE ) ~ ( FROM ~ ^#ident )?
        },
        |(_, _, _, opt_database)| {
            Statement::ShowDropTables(ShowDropTablesStmt {
                database: opt_database.map(|(_, database)| database),
            })
        },
    );

    let attach_table = map(
        rule! {
            ATTACH ~ TABLE ~ #dot_separated_idents_1_to_3 ~ #uri_location
        },
        |(_, _, (catalog, database, table), uri_location)| {
            Statement::AttachTable(AttachTableStmt {
                catalog,
                database,
                table,
                uri_location,
            })
        },
    );
    let create_table = map(
        rule! {
            CREATE ~ TRANSIENT? ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ ( #engine )?
            ~ ( #uri_location )?
            ~ ( CLUSTER ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" )?
            ~ ( #table_option )?
            ~ ( AS ~ ^#query )?
        },
        |(
            _,
            opt_transient,
            _,
            opt_if_not_exists,
            (catalog, database, table),
            source,
            engine,
            uri_location,
            opt_cluster_by,
            opt_table_options,
            opt_as_query,
        )| {
            Statement::CreateTable(CreateTableStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                catalog,
                database,
                table,
                source,
                engine,
                uri_location,
                cluster_by: opt_cluster_by
                    .map(|(_, _, _, exprs, _)| exprs)
                    .unwrap_or_default(),
                table_options: opt_table_options.unwrap_or_default(),
                as_query: opt_as_query.map(|(_, query)| Box::new(query)),
                transient: opt_transient.is_some(),
            })
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ ALL?
        },
        |(_, _, opt_if_exists, (catalog, database, table), opt_all)| {
            Statement::DropTable(DropTableStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                table,
                all: opt_all.is_some(),
            })
        },
    );
    let undrop_table = map(
        rule! {
            UNDROP ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::UndropTable(UndropTableStmt {
                catalog,
                database,
                table,
            })
        },
    );
    let alter_table = map(
        rule! {
            ALTER ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #table_reference_only ~ #alter_table_action
        },
        |(_, _, opt_if_exists, table_reference, action)| {
            Statement::AlterTable(AlterTableStmt {
                if_exists: opt_if_exists.is_some(),
                table_reference,
                action,
            })
        },
    );
    let rename_table = map(
        rule! {
            RENAME ~ TABLE ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ TO ~ #dot_separated_idents_1_to_3
        },
        |(
            _,
            _,
            opt_if_exists,
            (catalog, database, table),
            _,
            (new_catalog, new_database, new_table),
        )| {
            Statement::RenameTable(RenameTableStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                table,
                new_catalog,
                new_database,
                new_table,
            })
        },
    );
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::TruncateTable(TruncateTableStmt {
                catalog,
                database,
                table,
            })
        },
    );
    let optimize_table = map(
        rule! {
            OPTIMIZE ~ TABLE ~ #dot_separated_idents_1_to_3 ~ #optimize_table_action ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, _, (catalog, database, table), action, opt_limit)| {
            Statement::OptimizeTable(OptimizeTableStmt {
                catalog,
                database,
                table,
                action,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    );
    let vacuum_table = map(
        rule! {
            VACUUM ~ TABLE ~ #dot_separated_idents_1_to_3 ~ #vacuum_table_option
        },
        |(_, _, (catalog, database, table), option)| {
            Statement::VacuumTable(VacuumTableStmt {
                catalog,
                database,
                table,
                option,
            })
        },
    );
    let vacuum_drop_table = map(
        rule! {
            VACUUM ~ DROP ~ TABLE ~ (FROM ~ ^#dot_separated_idents_1_to_2)? ~ #vacuum_table_option
        },
        |(_, _, _, database_option, option)| {
            let (catalog, database) = database_option.map_or_else(
                || (None, None),
                |(_, catalog_database)| (catalog_database.0, Some(catalog_database.1)),
            );
            Statement::VacuumDropTable(VacuumDropTableStmt {
                catalog,
                database,
                option,
            })
        },
    );
    let analyze_table = map(
        rule! {
            ANALYZE ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::AnalyzeTable(AnalyzeTableStmt {
                catalog,
                database,
                table,
            })
        },
    );
    let exists_table = map(
        rule! {
            EXISTS ~ TABLE ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
            Statement::ExistsTable(ExistsTableStmt {
                catalog,
                database,
                table,
            })
        },
    );
    let create_view = map(
        rule! {
            CREATE ~ VIEW ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ AS ~ #query
        },
        |(_, _, opt_if_not_exists, (catalog, database, view), opt_columns, _, query)| {
            Statement::CreateView(CreateViewStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                catalog,
                database,
                view,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                query: Box::new(query),
            })
        },
    );
    let drop_view = map(
        rule! {
            DROP ~ VIEW ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, view))| {
            Statement::DropView(DropViewStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                view,
            })
        },
    );
    let alter_view = map(
        rule! {
            ALTER ~ VIEW
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ AS ~ #query
        },
        |(_, _, (catalog, database, view), opt_columns, _, query)| {
            Statement::AlterView(AlterViewStmt {
                catalog,
                database,
                view,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                query: Box::new(query),
            })
        },
    );

    let create_index = map(
        rule! {
            CREATE ~ SYNC? ~ AGGREGATING ~ INDEX ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ AS ~ #query
        },
        |(_, opt_sync, _, _, opt_if_not_exists, index_name, _, query)| {
            Statement::CreateIndex(CreateIndexStmt {
                index_type: TableIndexType::Aggregating,
                if_not_exists: opt_if_not_exists.is_some(),
                index_name,
                query: Box::new(query),
                sync_creation: opt_sync.is_some(),
            })
        },
    );

    let drop_index = map(
        rule! {
            DROP ~ AGGREGATING ~ INDEX ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, index)| {
            Statement::DropIndex(DropIndexStmt {
                if_exists: opt_if_exists.is_some(),
                index,
            })
        },
    );

    let refresh_index = map(
        rule! {
            REFRESH ~ AGGREGATING ~ INDEX ~ #ident ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, _, _, index, opt_limit)| {
            Statement::RefreshIndex(RefreshIndexStmt {
                index,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    );

    let create_virtual_column = map(
        rule! {
            CREATE ~ VIRTUAL ~ COLUMN ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, _, virtual_columns, _, _, (catalog, database, table))| {
            Statement::CreateVirtualColumn(CreateVirtualColumnStmt {
                catalog,
                database,
                table,
                virtual_columns,
            })
        },
    );

    let alter_virtual_column = map(
        rule! {
            ALTER ~ VIRTUAL ~ COLUMN ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, _, virtual_columns, _, _, (catalog, database, table))| {
            Statement::AlterVirtualColumn(AlterVirtualColumnStmt {
                catalog,
                database,
                table,
                virtual_columns,
            })
        },
    );

    let drop_virtual_column = map(
        rule! {
            DROP ~ VIRTUAL ~ COLUMN ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, _, (catalog, database, table))| {
            Statement::DropVirtualColumn(DropVirtualColumnStmt {
                catalog,
                database,
                table,
            })
        },
    );

    let refresh_virtual_column = map(
        rule! {
            REFRESH ~ VIRTUAL ~ COLUMN ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, _, (catalog, database, table))| {
            Statement::RefreshVirtualColumn(RefreshVirtualColumnStmt {
                catalog,
                database,
                table,
            })
        },
    );

    let show_users = value(Statement::ShowUsers, rule! { SHOW ~ USERS });
    let create_user = map(
        rule! {
            CREATE ~ USER ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #user_identity
            ~ IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )?
            ~ ( WITH ~ ^#comma_separated_list1(user_option))?
        },
        |(_, _, opt_if_not_exists, user, _, opt_auth_type, opt_password, opt_user_option)| {
            Statement::CreateUser(CreateUserStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                user,
                auth_option: AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                },
                user_options: opt_user_option
                    .map(|(_, user_options)| user_options)
                    .unwrap_or_default(),
            })
        },
    );
    let alter_user = map(
        rule! {
            ALTER ~ USER ~ ( #map(rule! { USER ~ "(" ~ ")" }, |_| None) | #map(user_identity, Some) )
            ~ ( IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )? )?
            ~ ( WITH ~ ^#comma_separated_list1(user_option) )?
        },
        |(_, _, user, opt_auth_option, opt_user_option)| {
            Statement::AlterUser(AlterUserStmt {
                user,
                auth_option: opt_auth_option.map(|(_, opt_auth_type, opt_password)| AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                }),
                user_options: opt_user_option
                    .map(|(_, user_options)| user_options)
                    .unwrap_or_default(),
            })
        },
    );
    let drop_user = map(
        rule! {
            DROP ~ USER ~ ( IF ~ ^EXISTS )? ~ #user_identity
        },
        |(_, _, opt_if_exists, user)| Statement::DropUser {
            if_exists: opt_if_exists.is_some(),
            user,
        },
    );
    let show_roles = value(Statement::ShowRoles, rule! { SHOW ~ ROLES });
    let create_role = map(
        rule! {
            CREATE ~ ROLE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #role_name
        },
        |(_, _, opt_if_not_exists, role_name)| Statement::CreateRole {
            if_not_exists: opt_if_not_exists.is_some(),
            role_name,
        },
    );
    let drop_role = map(
        rule! {
            DROP ~ ROLE ~ ( IF ~ ^EXISTS )? ~ #role_name
        },
        |(_, _, opt_if_exists, role_name)| Statement::DropRole {
            if_exists: opt_if_exists.is_some(),
            role_name,
        },
    );
    let grant = map(
        rule! {
            GRANT ~ #grant_source ~ TO ~ #grant_option
        },
        |(_, source, _, grant_option)| {
            Statement::Grant(GrantStmt {
                source,
                principal: grant_option,
            })
        },
    );
    let show_grants = map(
        rule! {
            SHOW ~ GRANTS ~ #show_grant_option?
        },
        |(_, _, show_grant_option)| match show_grant_option {
            Some(ShowGrantOption::PrincipalIdentity(principal)) => Statement::ShowGrants {
                principal: Some(principal),
            },
            Some(ShowGrantOption::ShareGrantObjectName(object)) => {
                Statement::ShowObjectGrantPrivileges(ShowObjectGrantPrivilegesStmt { object })
            }
            Some(ShowGrantOption::ShareName(share_name)) => {
                Statement::ShowGrantsOfShare(ShowGrantsOfShareStmt { share_name })
            }
            None => Statement::ShowGrants { principal: None },
        },
    );
    let revoke = map(
        rule! {
            REVOKE ~ #grant_source ~ FROM ~ #grant_option
        },
        |(_, source, _, grant_option)| {
            Statement::Revoke(RevokeStmt {
                source,
                principal: grant_option,
            })
        },
    );
    let create_udf = map(
        rule! {
            CREATE ~ FUNCTION ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #udf_definition
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, opt_if_not_exists, udf_name, definition, opt_description)| {
            Statement::CreateUDF(CreateUDFStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                udf_name,
                description: opt_description.map(|(_, _, description)| description),
                definition,
            })
        },
    );
    let drop_udf = map(
        rule! {
            DROP ~ FUNCTION ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, udf_name)| Statement::DropUDF {
            if_exists: opt_if_exists.is_some(),
            udf_name,
        },
    );
    let alter_udf = map(
        rule! {
            ALTER ~ FUNCTION
            ~ #ident ~ #udf_definition
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, udf_name, definition, opt_description)| {
            Statement::AlterUDF(AlterUDFStmt {
                udf_name,
                description: opt_description.map(|(_, _, description)| description),
                definition,
            })
        },
    );

    // stages
    let create_stage = map_res(
        rule! {
            CREATE ~ STAGE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ ( #stage_name )
            ~ ( URL ~ ^"=" ~ ^#uri_location )?
            ~ ( #file_format_clause )?
            ~ ( ON_ERROR ~ ^"=" ~ ^#ident )?
            ~ ( SIZE_LIMIT ~ ^"=" ~ ^#literal_u64 )?
            ~ ( VALIDATION_MODE ~ ^"=" ~ ^#ident )?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            stage,
            url_opt,
            file_format_opt,
            on_error_opt,
            size_limit_opt,
            validation_mode_opt,
            comment_opt,
        )| {
            Ok(Statement::CreateStage(CreateStageStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                stage_name: stage.to_string(),
                location: url_opt.map(|v| v.2),
                file_format_options: file_format_opt.unwrap_or_default(),
                on_error: on_error_opt.map(|v| v.2.to_string()).unwrap_or_default(),
                size_limit: size_limit_opt.map(|v| v.2 as usize).unwrap_or_default(),
                validation_mode: validation_mode_opt
                    .map(|v| v.2.to_string())
                    .unwrap_or_default(),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
            }))
        },
    );

    let list_stage = map(
        rule! {
            LIST ~ #at_string ~ (PATTERN ~ "=" ~ #literal_string)?
        },
        |(_, location, opt_pattern)| Statement::ListStage {
            location,
            pattern: opt_pattern.map(|v| v.2),
        },
    );

    let remove_stage = map(
        rule! {
            REMOVE ~ #at_string ~ (PATTERN ~ "=" ~ #literal_string)?
        },
        |(_, location, opt_pattern)| Statement::RemoveStage {
            location,
            pattern: opt_pattern.map(|v| v.2).unwrap_or_default(),
        },
    );

    let drop_stage = map(
        rule! {
            DROP ~ STAGE ~ ( IF ~ ^EXISTS )? ~ #stage_name
        },
        |(_, _, opt_if_exists, stage_name)| Statement::DropStage {
            if_exists: opt_if_exists.is_some(),
            stage_name: stage_name.to_string(),
        },
    );

    let desc_stage = map(
        rule! {
            (DESC | DESCRIBE) ~ STAGE ~ #ident
        },
        |(_, _, stage_name)| Statement::DescribeStage {
            stage_name: stage_name.to_string(),
        },
    );

    let call = map(
        rule! {
            CALL ~ #ident ~ "(" ~ #comma_separated_list0(parameter_to_string) ~ ")"
        },
        |(_, name, _, args, _)| {
            Statement::Call(CallStmt {
                name: name.to_string(),
                args,
            })
        },
    );

    let presign = map(
        rule! {
            PRESIGN ~ ( #presign_action )?
                ~ #presign_location
                ~ ( #presign_option )*
        },
        |(_, action, location, opts)| {
            let mut presign_stmt = PresignStmt {
                action: action.unwrap_or_default(),
                location,
                expire: Duration::from_secs(3600),
                content_type: None,
            };
            for opt in opts {
                presign_stmt.apply_option(opt);
            }
            Statement::Presign(presign_stmt)
        },
    );

    // share statements
    let create_share_endpoint = map(
        rule! {
            CREATE ~ SHARE ~ ENDPOINT ~ ( IF ~ ^NOT ~ ^EXISTS )?
             ~ #ident
             ~ URL ~ "=" ~ #share_endpoint_uri_location
             ~ TENANT ~ "=" ~ #ident
             ~ ( ARGS ~ ^"=" ~ ^#options)?
             ~ ( COMMENT ~ ^"=" ~ ^#literal_string)?
        },
        |(_, _, _, opt_if_not_exists, endpoint, _, _, url, _, _, tenant, args_opt, comment_opt)| {
            Statement::CreateShareEndpoint(CreateShareEndpointStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                endpoint,
                url,
                tenant,
                args: match args_opt {
                    Some(opt) => opt.2,
                    None => BTreeMap::new(),
                },
                comment: match comment_opt {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            })
        },
    );
    let show_share_endpoints = map(
        rule! {
            SHOW ~ SHARE ~ ENDPOINT
        },
        |(_, _, _)| Statement::ShowShareEndpoint(ShowShareEndpointStmt {}),
    );
    let drop_share_endpoint = map(
        rule! {
            DROP ~ SHARE ~ ENDPOINT ~ ( IF ~ EXISTS)? ~ #ident
        },
        |(_, _, _, opt_if_exists, endpoint)| {
            Statement::DropShareEndpoint(DropShareEndpointStmt {
                if_exists: opt_if_exists.is_some(),
                endpoint,
            })
        },
    );
    let create_share = map(
        rule! {
            CREATE ~ SHARE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ ( COMMENT ~ "=" ~ #literal_string)?
        },
        |(_, _, opt_if_not_exists, share, comment_opt)| {
            Statement::CreateShare(CreateShareStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                share,
                comment: match comment_opt {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            })
        },
    );
    let drop_share = map(
        rule! {
            DROP ~ SHARE ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, share)| {
            Statement::DropShare(DropShareStmt {
                if_exists: opt_if_exists.is_some(),
                share,
            })
        },
    );
    let grant_share_object = map(
        rule! {
            GRANT ~ #priv_share_type ~ ON ~ #grant_share_object_name ~ TO ~ SHARE ~ #ident
        },
        |(_, privilege, _, object, _, _, share)| {
            Statement::GrantShareObject(GrantShareObjectStmt {
                share,
                object,
                privilege,
            })
        },
    );
    let revoke_share_object = map(
        rule! {
            REVOKE ~ #priv_share_type ~ ON ~ #grant_share_object_name ~ FROM ~ SHARE ~ #ident
        },
        |(_, privilege, _, object, _, _, share)| {
            Statement::RevokeShareObject(RevokeShareObjectStmt {
                share,
                object,
                privilege,
            })
        },
    );
    let alter_share_tenants = map(
        rule! {
            ALTER ~ SHARE ~ ( IF ~ ^EXISTS )? ~ #ident ~ #alter_add_share_accounts ~ TENANTS ~ Eq ~ #comma_separated_list1(ident)
        },
        |(_, _, opt_if_exists, share, is_add, _, _, tenants)| {
            Statement::AlterShareTenants(AlterShareTenantsStmt {
                share,
                if_exists: opt_if_exists.is_some(),
                is_add,
                tenants,
            })
        },
    );
    let desc_share = map(
        rule! {
            (DESC | DESCRIBE) ~ SHARE ~ #ident
        },
        |(_, _, share)| Statement::DescShare(DescShareStmt { share }),
    );
    let show_shares = map(
        rule! {
            SHOW ~ SHARES
        },
        |(_, _)| Statement::ShowShares(ShowSharesStmt {}),
    );

    let create_file_format = map_res(
        rule! {
            CREATE ~ FILE ~ FORMAT ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #format_options
        },
        |(_, _, _, opt_if_not_exists, name, options)| {
            let file_format_options = FileFormatOptionsAst { options };
            Ok(Statement::CreateFileFormat {
                if_not_exists: opt_if_not_exists.is_some(),
                name: name.to_string(),
                file_format_options,
            })
        },
    );

    let drop_file_format = map(
        rule! {
            DROP ~ FILE ~ FORMAT ~ ( IF ~  EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, name)| Statement::DropFileFormat {
            if_exists: opt_if_exists.is_some(),
            name: name.to_string(),
        },
    );

    let show_file_formats = value(Statement::ShowFileFormats, rule! { SHOW ~ FILE ~ FORMATS });

    // data mark policy
    let create_data_mask_policy = map(
        rule! {
            CREATE ~ MASKING ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ #data_mask_policy
        },
        |(_, _, _, opt_if_not_exists, name, policy)| {
            let stmt = CreateDatamaskPolicyStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: name.to_string(),
                policy,
            };
            Statement::CreateDatamaskPolicy(stmt)
        },
    );
    let drop_data_mask_policy = map(
        rule! {
            DROP ~ MASKING ~ POLICY ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropDatamaskPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropDatamaskPolicy(stmt)
        },
    );
    let describe_data_mask_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ MASKING ~ POLICY ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescDatamaskPolicy(DescDatamaskPolicyStmt {
                name: name.to_string(),
            })
        },
    );

    let create_network_policy = map(
        rule! {
            CREATE ~ NETWORK ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident
             ~ ALLOWED_IP_LIST ~ Eq ~ "(" ~ ^#comma_separated_list0(literal_string) ~ ")"
             ~ ( BLOCKED_IP_LIST ~ Eq ~ "(" ~ ^#comma_separated_list0(literal_string) ~ ")" ) ?
             ~ ( COMMENT ~ Eq ~ #literal_string)?
        },
        |(
            _,
            _,
            _,
            opt_if_not_exists,
            name,
            _,
            _,
            _,
            allowed_ip_list,
            _,
            opt_blocked_ip_list,
            opt_comment,
        )| {
            let stmt = CreateNetworkPolicyStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: name.to_string(),
                allowed_ip_list,
                blocked_ip_list: match opt_blocked_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            };
            Statement::CreateNetworkPolicy(stmt)
        },
    );
    let alter_network_policy = map(
        rule! {
            ALTER ~ NETWORK ~ POLICY ~ ( IF ~ ^EXISTS )? ~ #ident ~ SET
             ~ ( ALLOWED_IP_LIST ~ Eq ~ "(" ~ ^#comma_separated_list0(literal_string) ~ ")" ) ?
             ~ ( BLOCKED_IP_LIST ~ Eq ~ "(" ~ ^#comma_separated_list0(literal_string) ~ ")" ) ?
             ~ ( COMMENT ~ Eq ~ #literal_string)?
        },
        |(
            _,
            _,
            _,
            opt_if_exists,
            name,
            _,
            opt_allowed_ip_list,
            opt_blocked_ip_list,
            opt_comment,
        )| {
            let stmt = AlterNetworkPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                allowed_ip_list: match opt_allowed_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                blocked_ip_list: match opt_blocked_ip_list {
                    Some(opt) => Some(opt.3),
                    None => None,
                },
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
            };
            Statement::AlterNetworkPolicy(stmt)
        },
    );
    let drop_network_policy = map(
        rule! {
            DROP ~ NETWORK ~ POLICY ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropNetworkPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropNetworkPolicy(stmt)
        },
    );
    let describe_network_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ NETWORK ~ POLICY ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescNetworkPolicy(DescNetworkPolicyStmt {
                name: name.to_string(),
            })
        },
    );
    let show_network_policies = value(
        Statement::ShowNetworkPolicies,
        rule! { SHOW ~ NETWORK ~ POLICIES },
    );

    let statement_body = alt((
        rule!(
            #map(query, |query| Statement::Query(Box::new(query)))
            | #explain : "`EXPLAIN [PIPELINE | GRAPH] <statement>`"
            | #explain_analyze : "`EXPLAIN ANALYZE <statement>`"
            | #delete : "`DELETE FROM <table> [WHERE ...]`"
            | #update : "`UPDATE <table> SET <column> = <expr> [, <column> = <expr> , ... ] [WHERE ...]`"
            | #show_settings : "`SHOW SETTINGS [<show_limit>]`"
            | #show_stages : "`SHOW STAGES`"
            | #show_engines : "`SHOW ENGINES`"
            | #show_process_list : "`SHOW PROCESSLIST`"
            | #show_metrics : "`SHOW METRICS`"
            | #show_functions : "`SHOW FUNCTIONS [<show_limit>]`"
            | #show_indexes : "`SHOW INDEXES`"
            | #kill_stmt : "`KILL (QUERY | CONNECTION) <object_id>`"
            | #set_role: "`SET [DEFAULT] ROLE <role>`"
            | #show_databases : "`SHOW [FULL] DATABASES [(FROM | IN) <catalog>] [<show_limit>]`"
            | #undrop_database : "`UNDROP DATABASE <database>`"
            | #show_create_database : "`SHOW CREATE DATABASE <database>`"
            | #create_database : "`CREATE DATABASE [IF NOT EXIST] <database> [ENGINE = <engine>]`"
            | #drop_database : "`DROP DATABASE [IF EXISTS] <database>`"
            | #alter_database : "`ALTER DATABASE [IF EXISTS] <action>`"
            | #use_database : "`USE <database>`"
        ),
        // network policy
        rule!(
            #create_network_policy: "`CREATE NETWORK POLICY [IF NOT EXISTS] name ALLOWED_IP_LIST = ('ip1' [, 'ip2']) [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
            | #alter_network_policy: "`ALTER NETWORK POLICY [IF EXISTS] name SET [ALLOWED_IP_LIST = ('ip1' [, 'ip2'])] [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
            | #drop_network_policy: "`DROP NETWORK POLICY [IF EXISTS] name`"
            | #describe_network_policy: "`DESC NETWORK POLICY name`"
            | #show_network_policies: "`SHOW NETWORK POLICIES`"
        ),
        rule!(
            #insert : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #replace : "`REPLACE INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #merge : "`MERGE INTO <target_table> USING <source> ON <join_expr> { matchedClause | notMatchedClause } [ ... ]`"
        ),
        rule!(
            #set_variable : "`SET <variable> = <value>`"
            | #unset_variable : "`UNSET <variable>`"
        ),
        rule!(
            #show_tables : "`SHOW [FULL] TABLES [FROM <database>] [<show_limit>]`"
            | #show_columns : "`SHOW [FULL] COLUMNS FROM <table> [FROM|IN <catalog>.<database>] [<show_limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #describe_table : "`DESCRIBE [<database>.]<table>`"
            | #show_fields : "`SHOW FIELDS FROM [<database>.]<table>`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show_limit>]`"
            | #show_drop_tables_status : "`SHOW DROP TABLES [FROM <database>]`"
            | #attach_table : "`ATTACH TABLE [<database>.]<table> <uri>`"
            | #create_table : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> [<source>] [<table_options>]`"
            | #drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
            | #undrop_table : "`UNDROP TABLE [<database>.]<table>`"
            | #alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #truncate_table : "`TRUNCATE TABLE [<database>.]<table>`"
            | #optimize_table : "`OPTIMIZE TABLE [<database>.]<table> (ALL | PURGE | COMPACT [SEGMENT])`"
            | #vacuum_table : "`VACUUM TABLE [<database>.]<table> [RETAIN number HOURS] [DRY RUN]`"
            | #vacuum_drop_table : "`VACUUM DROP TABLE [FROM [<catalog>.]<database>] [RETAIN number HOURS] [DRY RUN]`"
            | #analyze_table : "`ANALYZE TABLE [<database>.]<table>`"
            | #exists_table : "`EXISTS TABLE [<database>.]<table>`"
            | #show_table_functions : "`SHOW TABLE_FUNCTIONS [<show_limit>]`"
        ),
        rule!(
            #create_view : "`CREATE VIEW [IF NOT EXISTS] [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
            | #drop_view : "`DROP VIEW [IF EXISTS] [<database>.]<view>`"
            | #alter_view : "`ALTER VIEW [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
        ),
        rule!(
            #create_index: "`CREATE AGGREGATING INDEX [IF NOT EXISTS] <index> AS SELECT ...`"
            | #drop_index: "`DROP AGGREGATING INDEX [IF EXISTS] <index>`"
            | #refresh_index: "`REFRESH AGGREGATING INDEX <index> [LIMIT <limit>]`"
        ),
        rule!(
            #create_virtual_column: "`CREATE VIRTUAL COLUMN (expr, ...) FOR [<database>.]<table>`"
            | #alter_virtual_column: "`ALTER VIRTUAL COLUMN (expr, ...) FOR [<database>.]<table>`"
            | #drop_virtual_column: "`DROP VIRTUAL COLUMN FOR [<database>.]<table>`"
            | #refresh_virtual_column: "`REFRESH VIRTUAL COLUMN FOR [<database>.]<table>`"
        ),
        rule!(
            #show_users : "`SHOW USERS`"
            | #create_user : "`CREATE USER [IF NOT EXISTS] '<username>'@'hostname' IDENTIFIED [WITH <auth_type>] [BY <password>] [WITH <user_option>, ...]`"
            | #alter_user : "`ALTER USER ('<username>'@'hostname' | USER()) [IDENTIFIED [WITH <auth_type>] [BY <password>]] [WITH <user_option>, ...]`"
            | #drop_user : "`DROP USER [IF EXISTS] '<username>'@'hostname'`"
            | #show_roles : "`SHOW ROLES`"
            | #create_role : "`CREATE ROLE [IF NOT EXISTS] <role_name>`"
            | #drop_role : "`DROP ROLE [IF EXISTS] <role_name>`"
            | #create_udf : "`CREATE FUNCTION [IF NOT EXISTS] <name> {AS (<parameter>, ...) -> <definition expr> | (<arg_type>, ...) RETURNS <return_type> LANGUAGE <language> HANDLER=<handler> ADDRESS=<udf_server_address>} [DESC = <description>]`"
            | #drop_udf : "`DROP FUNCTION [IF EXISTS] <udf_name>`"
            | #alter_udf : "`ALTER FUNCTION <udf_name> (<parameter>, ...) -> <definition_expr> [DESC = <description>]`"
        ),
        rule!(
            #create_stage: "`CREATE STAGE [ IF NOT EXISTS ] <stage_name>
                [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
                [ COPY_OPTIONS = ( copyOptions ) ]
                [ COMMENT = '<string_literal>' ]`"
            | #desc_stage: "`DESC STAGE <stage_name>`"
            | #list_stage: "`LIST @<stage_name> [pattern = '<pattern>']`"
            | #remove_stage: "`REMOVE @<stage_name> [pattern = '<pattern>']`"
            | #drop_stage: "`DROP STAGE <stage_name>`"
        ),
        rule!(
            #create_file_format: "`CREATE FILE FORMAT [ IF NOT EXISTS ] <format_name> formatTypeOptions`"
            | #show_file_formats: "`SHOW FILE FORMATS`"
            | #drop_file_format: "`DROP FILE FORMAT  [ IF EXISTS ] <format_name>`"
        ),
        rule!( #copy_into ),
        rule!(
            #call: "`CALL <procedure_name>(<parameter>, ...)`"
        ),
        rule!(
            #grant : "`GRANT { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } TO { [ROLE <role_name>] | [USER] <user> }`"
            | #show_grants : "`SHOW GRANTS {FOR  { ROLE <role_name> | USER <user> }] | ON {DATABASE <db_name> | TABLE <db_name>.<table_name>} }`"
            | #revoke : "`REVOKE { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } FROM { [ROLE <role_name>] | [USER] <user> }`"
        ),
        rule!(
            #presign: "`PRESIGN [{DOWNLOAD | UPLOAD}] <location> [EXPIRE = 3600]`"
        ),
        // data mask
        rule!(
            #create_data_mask_policy: "`CREATE MASKING POLICY [IF NOT EXISTS] mask_name as (val1 val_type1 [, val type]) return type -> case`"
            | #drop_data_mask_policy: "`DROP MASKING POLICY [IF EXISTS] mask_name`"
            | #describe_data_mask_policy: "`DESC MASKING POLICY mask_name`"
        ),
        // share
        rule!(
            #create_share_endpoint: "`CREATE SHARE ENDPOINT [IF NOT EXISTS] <endpoint_name> URL=endpoint_location tenant=tenant_name ARGS=(arg=..) [ COMMENT = '<string_literal>' ]`"
            | #show_share_endpoints: "`SHOW SHARE ENDPOINT`"
            | #drop_share_endpoint: "`DROP SHARE ENDPOINT <endpoint_name>`"
            | #create_share: "`CREATE SHARE [IF NOT EXISTS] <share_name> [ COMMENT = '<string_literal>' ]`"
            | #drop_share: "`DROP SHARE [IF EXISTS] <share_name>`"
            | #grant_share_object: "`GRANT { USAGE | SELECT | REFERENCE_USAGE } ON { DATABASE db | TABLE db.table } TO SHARE <share_name>`"
            | #revoke_share_object: "`REVOKE { USAGE | SELECT | REFERENCE_USAGE } ON { DATABASE db | TABLE db.table } FROM SHARE <share_name>`"
            | #alter_share_tenants: "`ALTER SHARE [IF EXISTS] <share_name> { ADD | REMOVE } TENANTS = tenant [, tenant, ...]`"
            | #desc_share: "`{DESC | DESCRIBE} SHARE <share_name>`"
            | #show_shares: "`SHOW SHARES`"
        ),
        // catalog
        rule!(
         #show_catalogs : "`SHOW CATALOGS [<show_limit>]`"
        | #show_create_catalog : "`SHOW CREATE CATALOG <catalog>`"
        | #create_catalog: "`CREATE CATALOG [IF NOT EXISTS] <catalog> TYPE=<catalog_type> CONNECTION=<catalog_options>`"
        | #drop_catalog: "`DROP CATALOG [IF EXISTS] <catalog>`"
        ),
        rule!(
            #create_task : "`CREATE TASK [ IF NOT EXISTS ] <name>
  [ { WAREHOUSE = <string> }
  [ SCHEDULE = { <num> MINUTE | USING CRON <expr> <time_zone> } ]
  [ SUSPEND_TASK_AFTER_NUM_FAILURES = <num> ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
        ),
    ));

    map(
        rule! {
            #statement_body ~ ( FORMAT ~ ^#ident )? ~ ";"? ~ &EOI
        },
        |(stmt, opt_format, _, _)| StatementMsg {
            stmt,
            format: opt_format.map(|(_, format)| format.name),
        },
    )(i)
}

// `INSERT INTO ... FORMAT ...` and `INSERT INTO ... VALUES` statements will
// stop the parser immediately and return the rest tokens by `InsertSource`.
//
// This is a hack to make it able to parse a large streaming insert statement.
pub fn insert_source(i: Input) -> IResult<InsertSource> {
    let streaming = map(
        rule! {
            FORMAT ~ #ident ~ #rest_str
        },
        |(_, format, (rest_str, start))| InsertSource::Streaming {
            format: format.name,
            rest_str,
            start,
        },
    );
    let streaming_v2 = map(
        rule! {
           #file_format_clause ~ (ON_ERROR ~ ^"=" ~ ^#ident)? ~  #rest_str
        },
        |(options, on_error_opt, (_, start))| InsertSource::StreamingV2 {
            settings: options,
            on_error_mode: on_error_opt.map(|v| v.2.to_string()),
            start,
        },
    );
    let values = map(
        rule! {
            VALUES ~ #rest_str
        },
        |(_, (rest_str, start))| InsertSource::Values { rest_str, start },
    );
    let query = map(query, |query| InsertSource::Select {
        query: Box::new(query),
    });

    rule!(
        #streaming
        | #streaming_v2
        | #values
        | #query
    )(i)
}

pub fn merge_source(i: Input) -> IResult<MergeSource> {
    let streaming_v2 = map(
        rule! {
           #file_format_clause  ~ (ON_ERROR ~ ^"=" ~ ^#ident)? ~  #rest_str
        },
        |(options, on_error_opt, (_, start))| MergeSource::StreamingV2 {
            settings: options,
            on_error_mode: on_error_opt.map(|v| v.2.to_string()),
            start,
        },
    );

    let query = map(rule! {#query ~ #table_alias}, |(query, source_alias)| {
        MergeSource::Select {
            query: Box::new(query),
            source_alias,
        }
    });

    rule!(
          #streaming_v2
        | #query
    )(i)
}

pub fn unset_source(i: Input) -> IResult<UnSetSource> {
    //#ident ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ")")?
    let var = map(
        rule! {
            #ident
        },
        |variable| UnSetSource::Var { variable },
    );
    let vars = map(
        rule! {
            "(" ~ ^#comma_separated_list1(ident) ~ ")"
        },
        |(_, variables, _)| UnSetSource::Vars { variables },
    );

    rule!(
        #var
        | #vars
    )(i)
}

pub fn set_var_hints(i: Input) -> IResult<HintItem> {
    map(
        rule! {
            SET_VAR ~ ^"(" ~ ^#ident ~ ^"=" ~ #subexpr(0) ~ ^")"
        },
        |(_, _, name, _, expr, _)| HintItem { name, expr },
    )(i)
}

pub fn hint(i: Input) -> IResult<Hint> {
    let hint = map(
        rule! {
            "/*+" ~ #set_var_hints+ ~ "*/"
        },
        |(_, hints_list, _)| Hint { hints_list },
    );
    let invalid_hint = map(
        rule! {
            "/*+" ~ (!"*/" ~ #any_token)* ~ "*/"
        },
        |_| Hint { hints_list: vec![] },
    );
    rule!(#hint|#invalid_hint)(i)
}

pub fn rest_str(i: Input) -> IResult<(String, usize)> {
    // It's safe to unwrap because input must contain EOI.
    let first_token = i.0.first().unwrap();
    let last_token = i.0.last().unwrap();
    Ok((
        i.slice((i.len() - 1)..),
        (
            first_token.source[first_token.span.start()..last_token.span.end()].to_string(),
            first_token.span.start(),
        ),
    ))
}

pub fn column_def(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultExpr(Box<Expr>),
        VirtualExpr(Box<Expr>),
        StoredExpr(Box<Expr>),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let expr = alt((
        map(
            rule! {
                DEFAULT ~ ^#subexpr(NOT_PREC)
            },
            |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
        ),
        map(
            rule! {
                (GENERATED ~ ^ALWAYS)? ~ AS ~ ^"(" ~ ^#subexpr(NOT_PREC) ~ ^")" ~ VIRTUAL
            },
            |(_, _, _, virtual_expr, _, _)| ColumnConstraint::VirtualExpr(Box::new(virtual_expr)),
        ),
        map(
            rule! {
                (GENERATED ~ ^ALWAYS)? ~ AS ~ ^"(" ~ ^#subexpr(NOT_PREC) ~ ^")" ~ STORED
            },
            |(_, _, _, stored_expr, _, _)| ColumnConstraint::StoredExpr(Box::new(stored_expr)),
        ),
    ));

    let comment = map(
        rule! {
            COMMENT ~ #literal_string
        },
        |(_, comment)| comment,
    );

    let (i, (mut def, constraints)) = map(
        rule! {
            #ident
            ~ #type_name
            ~ ( #nullable | #expr )*
            ~ ( #comment )?
            : "`<column name> <type> [DEFAULT <expr>] [AS (<expr>) VIRTUAL] [AS (<expr>) STORED] [COMMENT '<comment>']`"
        },
        |(name, data_type, constraints, comment)| {
            let def = ColumnDefinition {
                name,
                data_type,
                expr: None,
                comment,
                nullable_constraint: None,
            };
            (def, constraints)
        },
    )(i)?;

    for constraint in constraints {
        match constraint {
            ColumnConstraint::Nullable(nullable) => {
                if nullable {
                    def.data_type = def.data_type.wrap_nullable();
                    def.nullable_constraint = Some(NullableConstraint::Null);
                } else if def.data_type.is_nullable() {
                    return Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other("ambiguous NOT NULL constraint"),
                    )));
                } else {
                    def.nullable_constraint = Some(NullableConstraint::NotNull);
                }
            }
            ColumnConstraint::DefaultExpr(default_expr) => {
                def.expr = Some(ColumnExpr::Default(default_expr))
            }
            ColumnConstraint::VirtualExpr(virtual_expr) => {
                def.expr = Some(ColumnExpr::Virtual(virtual_expr))
            }
            ColumnConstraint::StoredExpr(stored_expr) => {
                def.expr = Some(ColumnExpr::Stored(stored_expr))
            }
        }
    }

    Ok((i, def))
}

pub fn role_name(i: Input) -> IResult<String> {
    let role_ident = map(
        rule! {
            #ident
        },
        |role_name| role_name.name,
    );
    let role_lit = map(
        rule! {
            #literal_string
        },
        |role_name| role_name,
    );

    rule!(
        #role_ident : "<role_name>"
        | #role_lit : "'<role_name>'"
    )(i)
}

pub fn grant_source(i: Input) -> IResult<AccountMgrSource> {
    let role = map(
        rule! {
            ROLE ~ #role_name
        },
        |(_, role_name)| AccountMgrSource::Role { role: role_name },
    );
    let privs = map(
        rule! {
            #comma_separated_list1(priv_type) ~ ON ~ #grant_level
        },
        |(privs, _, level)| AccountMgrSource::Privs {
            privileges: privs,
            level,
        },
    );
    let all = map(
        rule! { ALL ~ PRIVILEGES? ~ ON ~ #grant_level },
        |(_, _, _, level)| AccountMgrSource::ALL { level },
    );

    rule!(
        #role : "ROLE <role_name>"
        | #privs : "<privileges> ON <privileges_level>"
        | #all : "ALL [ PRIVILEGES ] ON <privileges_level>"
    )(i)
}

pub fn priv_type(i: Input) -> IResult<UserPrivilegeType> {
    alt((
        value(UserPrivilegeType::Usage, rule! { USAGE }),
        value(UserPrivilegeType::Select, rule! { SELECT }),
        value(UserPrivilegeType::Insert, rule! { INSERT }),
        value(UserPrivilegeType::Update, rule! { UPDATE }),
        value(UserPrivilegeType::Delete, rule! { DELETE }),
        value(UserPrivilegeType::Alter, rule! { ALTER }),
        value(UserPrivilegeType::Super, rule! { SUPER }),
        value(UserPrivilegeType::CreateUser, rule! { CREATE ~ USER }),
        value(UserPrivilegeType::DropUser, rule! { DROP ~ USER }),
        value(UserPrivilegeType::CreateRole, rule! { CREATE ~ ROLE }),
        value(UserPrivilegeType::DropRole, rule! { DROP ~ ROLE }),
        value(UserPrivilegeType::Grant, rule! { GRANT }),
        value(UserPrivilegeType::CreateStage, rule! { CREATE ~ STAGE }),
        value(UserPrivilegeType::Set, rule! { SET }),
        value(UserPrivilegeType::Drop, rule! { DROP }),
        value(UserPrivilegeType::Create, rule! { CREATE }),
        value(UserPrivilegeType::Ownership, rule! { OWNERSHIP }),
    ))(i)
}

pub fn priv_share_type(i: Input) -> IResult<ShareGrantObjectPrivilege> {
    alt((
        value(ShareGrantObjectPrivilege::Usage, rule! { USAGE }),
        value(ShareGrantObjectPrivilege::Select, rule! { SELECT }),
        value(
            ShareGrantObjectPrivilege::ReferenceUsage,
            rule! { REFERENCE_USAGE },
        ),
    ))(i)
}

pub fn alter_add_share_accounts(i: Input) -> IResult<bool> {
    alt((value(true, rule! { ADD }), value(false, rule! { REMOVE })))(i)
}

pub fn grant_share_object_name(i: Input) -> IResult<ShareGrantObjectName> {
    let database = map(
        rule! {
            DATABASE ~ #ident
        },
        |(_, database)| ShareGrantObjectName::Database(database.to_string()),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            TABLE ~  #ident ~ "." ~ #ident
        },
        |(_, database, _, table)| {
            ShareGrantObjectName::Table(database.to_string(), table.to_string())
        },
    );

    rule!(
        #database : "DATABASE <database>"
        | #table : "TABLE <database>.<table>"
    )(i)
}

pub fn grant_level(i: Input) -> IResult<AccountMgrLevel> {
    // *.*
    let global = map(rule! { "*" ~ "." ~ "*" }, |_| AccountMgrLevel::Global);
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #ident ~ "." )? ~ "*"
        },
        |(database, _)| AccountMgrLevel::Database(database.map(|(database, _)| database.name)),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            ( #ident ~ "." )? ~ #parameter_to_string
        },
        |(database, table)| {
            AccountMgrLevel::Table(database.map(|(database, _)| database.name), table)
        },
    );

    rule!(
        #global : "*.*"
        | #db : "<database>.*"
        | #table : "<database>.<table>"
    )(i)
}

pub fn show_grant_option(i: Input) -> IResult<ShowGrantOption> {
    let grant_role = map(
        rule! {
            FOR ~ #grant_option
        },
        |(_, opt_principal)| ShowGrantOption::PrincipalIdentity(opt_principal),
    );

    let share_object_name = map(
        rule! {
            ON ~ #grant_share_object_name
        },
        |(_, object_name)| ShowGrantOption::ShareGrantObjectName(object_name),
    );

    let share_name = map(
        rule! {
            OF ~ SHARE ~ #ident
        },
        |(_, _, share_name)| ShowGrantOption::ShareName(share_name.to_string()),
    );

    rule!(
        #grant_role: "FOR  { ROLE <role_name> | [USER] <user> }"
        | #share_object_name: "ON {DATABASE <db_name> | TABLE <db_name>.<table_name>}"
        | #share_name: "OF SHARE <share_name>"
    )(i)
}

pub fn grant_option(i: Input) -> IResult<PrincipalIdentity> {
    let role = map(
        rule! {
            ROLE ~ #role_name
        },
        |(_, role_name)| PrincipalIdentity::Role(role_name),
    );

    let user = map(
        rule! {
            USER? ~ #user_identity
        },
        |(_, user)| PrincipalIdentity::User(user),
    );

    rule!(
        #role
        | #user
    )(i)
}

pub fn create_table_source(i: Input) -> IResult<CreateTableSource> {
    let columns = map(
        rule! {
            "(" ~ ^#comma_separated_list1(column_def) ~ ^")"
        },
        |(_, columns, _)| CreateTableSource::Columns(columns),
    );
    let like = map(
        rule! {
            LIKE ~ #dot_separated_idents_1_to_3
        },
        |(_, (catalog, database, table))| CreateTableSource::Like {
            catalog,
            database,
            table,
        },
    );

    rule!(
        #columns
        | #like
    )(i)
}

pub fn alter_database_action(i: Input) -> IResult<AlterDatabaseAction> {
    let mut rename_database = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_db)| AlterDatabaseAction::RenameDatabase { new_db },
    );

    rule!(
        #rename_database
    )(i)
}

pub fn modify_column_type(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultExpr(Box<Expr>),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let expr = alt((map(
        rule! {
            DEFAULT ~ ^#subexpr(NOT_PREC)
        },
        |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
    ),));

    let comment = map(
        rule! {
            COMMENT ~ #literal_string
        },
        |(_, comment)| comment,
    );

    map(
        rule! {
            #ident
            ~ #type_name
            ~ ( #nullable | #expr )*
            ~ ( #comment )?
            : "`<column name> <type> [DEFAULT <expr>] [COMMENT '<comment>']`"
        },
        |(name, data_type, constraints, comment)| {
            let mut def = ColumnDefinition {
                name,
                data_type,
                expr: None,
                comment,
                nullable_constraint: None,
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::Nullable(nullable) => {
                        if nullable {
                            def.data_type = def.data_type.wrap_nullable();
                            def.nullable_constraint = Some(NullableConstraint::Null);
                        } else {
                            def.nullable_constraint = Some(NullableConstraint::NotNull);
                        }
                    }
                    ColumnConstraint::DefaultExpr(default_expr) => {
                        def.expr = Some(ColumnExpr::Default(default_expr))
                    }
                }
            }
            def
        },
    )(i)
}

pub fn modify_column_action(i: Input) -> IResult<ModifyColumnAction> {
    let set_mask_policy = map(
        rule! {
            #ident ~ SET ~ MASKING ~ POLICY ~ #ident
        },
        |(column, _, _, _, mask_name)| {
            ModifyColumnAction::SetMaskingPolicy(column, mask_name.to_string())
        },
    );

    let unset_mask_policy = map(
        rule! {
            #ident ~ UNSET ~ MASKING ~ POLICY
        },
        |(column, _, _, _)| ModifyColumnAction::UnsetMaskingPolicy(column),
    );

    let convert_stored_computed_column = map(
        rule! {
            #ident ~ DROP ~ STORED
        },
        |(column, _, _)| ModifyColumnAction::ConvertStoredComputedColumn(column),
    );

    let modify_column_type = map(
        rule! {
            #modify_column_type ~ ("," ~ COLUMN ~ #modify_column_type)*
        },
        |(column_def, column_def_vec)| {
            let mut column_defs = vec![column_def];
            column_def_vec
                .iter()
                .for_each(|(_, _, column_def)| column_defs.push(column_def.clone()));
            ModifyColumnAction::SetDataType(column_defs)
        },
    );

    rule!(
        #set_mask_policy
        | #unset_mask_policy
        | #convert_stored_computed_column
        | #modify_column_type
    )(i)
}

pub fn alter_table_action(i: Input) -> IResult<AlterTableAction> {
    let rename_table = map(
        rule! {
           RENAME ~ TO ~ #ident
        },
        |(_, _, new_table)| AlterTableAction::RenameTable { new_table },
    );
    let rename_column = map(
        rule! {
            RENAME ~ COLUMN ~ #ident ~ TO ~ #ident
        },
        |(_, _, old_column, _, new_column)| AlterTableAction::RenameColumn {
            old_column,
            new_column,
        },
    );
    let add_column = map(
        rule! {
            ADD ~ COLUMN ~ #column_def ~ ( #add_column_option )?
        },
        |(_, _, column, option)| AlterTableAction::AddColumn {
            column,
            option: option.unwrap_or(AddColumnOption::End),
        },
    );

    let modify_column = map(
        rule! {
            MODIFY ~ COLUMN ~ #modify_column_action
        },
        |(_, _, action)| AlterTableAction::ModifyColumn { action },
    );

    let drop_column = map(
        rule! {
            DROP ~ COLUMN ~ #ident
        },
        |(_, _, column)| AlterTableAction::DropColumn { column },
    );
    let alter_table_cluster_key = map(
        rule! {
            CLUSTER ~ ^BY ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")"
        },
        |(_, _, _, cluster_by, _)| AlterTableAction::AlterTableClusterKey { cluster_by },
    );

    let drop_table_cluster_key = map(
        rule! {
            DROP ~ CLUSTER ~ KEY
        },
        |(_, _, _)| AlterTableAction::DropTableClusterKey,
    );

    let recluster_table = map(
        rule! {
            RECLUSTER ~ FINAL? ~ ( WHERE ~ ^#expr )? ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, opt_is_final, opt_selection, opt_limit)| AlterTableAction::ReclusterTable {
            is_final: opt_is_final.is_some(),
            selection: opt_selection.map(|(_, selection)| selection),
            limit: opt_limit.map(|(_, limit)| limit),
        },
    );

    let revert_table = map(
        rule! {
            FLASHBACK ~ TO ~ #travel_point
        },
        |(_, _, point)| AlterTableAction::RevertTo { point },
    );

    let set_table_options = map(
        rule! {
            SET ~ OPTIONS ~ "(" ~ #set_table_option ~ ")"
        },
        |(_, _, _, set_options, _)| AlterTableAction::SetOptions { set_options },
    );

    rule!(
        #rename_table
        | #rename_column
        | #add_column
        | #drop_column
        | #modify_column
        | #alter_table_cluster_key
        | #drop_table_cluster_key
        | #recluster_table
        | #revert_table
        | #set_table_options
    )(i)
}

pub fn match_clause(i: Input) -> IResult<MergeOption> {
    map(
        rule! {
            WHEN ~ MATCHED ~ (AND ~ ^#expr)? ~ THEN ~ #match_operation
        },
        |(_, _, expr_op, _, match_operation)| match expr_op {
            Some(expr) => MergeOption::Match(MatchedClause {
                selection: Some(expr.1),
                operation: match_operation,
            }),
            None => MergeOption::Match(MatchedClause {
                selection: None,
                operation: match_operation,
            }),
        },
    )(i)
}

fn match_operation(i: Input) -> IResult<MatchOperation> {
    alt((
        value(MatchOperation::Delete, rule! {DELETE}),
        map(
            rule! {
                UPDATE ~ SET ~ ^#comma_separated_list1(merge_update_expr)
            },
            |(_, _, update_list)| MatchOperation::Update {
                update_list,
                is_star: false,
            },
        ),
        map(
            rule! {
                UPDATE ~ "*"
            },
            |(_, _)| MatchOperation::Update {
                update_list: Vec::new(),
                is_star: true,
            },
        ),
    ))(i)
}

pub fn unmatch_clause(i: Input) -> IResult<MergeOption> {
    alt((
        map(
            rule! {
                WHEN ~ NOT ~ MATCHED ~ (AND ~ ^#expr)?  ~ THEN ~ INSERT ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ^")" )?
                ~ VALUES ~ ^#row_values
            },
            |(_, _, _, expr_op, _, _, columns_op, _, values)| {
                let selection = match expr_op {
                    Some(e) => Some(e.1),
                    None => None,
                };
                match columns_op {
                    Some(columns) => MergeOption::Unmatch(UnmatchedClause {
                        insert_operation: InsertOperation {
                            columns: Some(columns.1),
                            values,
                            is_star: false,
                        },
                        selection,
                    }),
                    None => MergeOption::Unmatch(UnmatchedClause {
                        insert_operation: InsertOperation {
                            columns: None,
                            values,
                            is_star: false,
                        },
                        selection,
                    }),
                }
            },
        ),
        map(
            rule! {
                WHEN ~ NOT ~ MATCHED ~ (AND ~ ^#expr)?  ~ THEN ~ INSERT ~ "*"
            },
            |(_, _, _, expr_op, _, _, _)| {
                let selection = match expr_op {
                    Some(e) => Some(e.1),
                    None => None,
                };
                MergeOption::Unmatch(UnmatchedClause {
                    insert_operation: InsertOperation {
                        columns: None,
                        values: Vec::new(),
                        is_star: true,
                    },
                    selection,
                })
            },
        ),
    ))(i)
}

pub fn add_column_option(i: Input) -> IResult<AddColumnOption> {
    alt((
        value(AddColumnOption::First, rule! { FIRST }),
        map(rule! { AFTER ~ #ident }, |(_, ident)| {
            AddColumnOption::After(ident)
        }),
    ))(i)
}

pub fn optimize_table_action(i: Input) -> IResult<OptimizeTableAction> {
    alt((
        value(OptimizeTableAction::All, rule! { ALL }),
        map(
            rule! { PURGE ~ (BEFORE ~ ^#travel_point)? },
            |(_, opt_travel_point)| OptimizeTableAction::Purge {
                before: opt_travel_point.map(|(_, p)| p),
            },
        ),
        map(rule! { COMPACT ~ SEGMENT? }, |(_, opt_segment)| {
            OptimizeTableAction::Compact {
                target: opt_segment.map_or(CompactTarget::Block, |_| CompactTarget::Segment),
            }
        }),
    ))(i)
}

pub fn vacuum_table_option(i: Input) -> IResult<VacuumTableOption> {
    alt((map(
        rule! {
            (RETAIN ~ ^#expr ~ ^HOURS)? ~ (DRY ~ ^RUN)?
        },
        |(retain_hours_opt, dry_run_opt)| {
            let retain_hours = match retain_hours_opt {
                Some(retain_hours) => Some(retain_hours.1),
                None => None,
            };
            let dry_run = match dry_run_opt {
                Some(_) => Some(()),
                None => None,
            };
            VacuumTableOption {
                retain_hours,
                dry_run,
            }
        },
    ),))(i)
}

pub fn task_warehouse_option(i: Input) -> IResult<WarehouseOptions> {
    alt((map(
        rule! {
            (WAREHOUSE  ~ "=" ~  #literal_string)?
        },
        |warehouse_opt| {
            let warehouse = match warehouse_opt {
                Some(warehouse) => Some(warehouse.2),
                None => None,
            };
            WarehouseOptions { warehouse }
        },
    ),))(i)
}

pub fn task_schedule_option(i: Input) -> IResult<ScheduleOptions> {
    let interval = map(
        rule! {
             #literal_u64 ~ MINUTE
        },
        |(minutes, _)| ScheduleOptions::IntervalMinutes(minutes),
    );
    let cron_expr = map(
        rule! {
            USING ~ CRON ~ #literal_string ~ #literal_string?
        },
        |(_, _, expr, timezone)| ScheduleOptions::CronExpression(expr, timezone),
    );

    rule!(
        #interval
        | #cron_expr
    )(i)
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

pub fn table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! {
           ( #ident ~ "=" ~ #parameter_to_string )*
        },
        |opts| {
            BTreeMap::from_iter(
                opts.iter()
                    .map(|(k, _, v)| (k.name.to_lowercase(), v.clone())),
            )
        },
    )(i)
}

pub fn set_table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! {
           ( #ident ~ "=" ~ #parameter_to_string ) ~ ("," ~ #ident ~ "=" ~ #parameter_to_string )*
        },
        |(key, _, value, opts)| {
            let mut options = BTreeMap::from_iter(
                opts.iter()
                    .map(|(_, k, _, v)| (k.name.to_lowercase(), v.clone())),
            );
            options.insert(key.name.to_lowercase(), value);
            options
        },
    )(i)
}

pub fn engine(i: Input) -> IResult<Engine> {
    let engine = alt((
        value(Engine::Null, rule! { NULL }),
        value(Engine::Memory, rule! { MEMORY }),
        value(Engine::Fuse, rule! { FUSE }),
        value(Engine::View, rule! { VIEW }),
        value(Engine::Random, rule! { RANDOM }),
    ));

    map(
        rule! {
            ENGINE ~ ^"=" ~ ^#engine
        },
        |(_, _, engine)| engine,
    )(i)
}

pub fn database_engine(i: Input) -> IResult<DatabaseEngine> {
    let engine = alt((value(DatabaseEngine::Default, rule! {DEFAULT}),));

    map(
        rule! {
            ^#engine
        },
        |engine| engine,
    )(i)
}

pub fn create_database_option(i: Input) -> IResult<CreateDatabaseOption> {
    let create_db_engine = alt((map(
        rule! {
            ^#database_engine
        },
        CreateDatabaseOption::DatabaseEngine,
    ),));

    let share_from = alt((map(
        rule! {
            #ident ~ "." ~ #ident
        },
        |(tenant, _, share_name)| {
            CreateDatabaseOption::FromShare(ShareNameIdent {
                tenant: tenant.to_string(),
                share_name: share_name.to_string(),
            })
        },
    ),));

    map(
        rule! {
            ENGINE ~  ^"=" ~ ^#create_db_engine
            | FROM ~ SHARE ~ ^#share_from
        },
        |(_, _, option)| option,
    )(i)
}

pub fn catalog_type(i: Input) -> IResult<CatalogType> {
    let catalog_type = alt((
        value(CatalogType::Default, rule! {DEFAULT}),
        value(CatalogType::Hive, rule! {HIVE}),
        value(CatalogType::Iceberg, rule! {ICEBERG}),
    ));
    map(rule! { ^#catalog_type }, |catalog_type| catalog_type)(i)
}

pub fn user_option(i: Input) -> IResult<UserOptionItem> {
    let default_role_option = map(
        rule! {
            "DEFAULT_ROLE" ~ "=" ~ #role_name
        },
        |(_, _, role)| UserOptionItem::DefaultRole(role),
    );
    let set_network_policy = map(
        rule! {
            SET ~ NETWORK ~ POLICY ~ "=" ~ #literal_string
        },
        |(_, _, _, _, policy)| UserOptionItem::SetNetworkPolicy(policy),
    );
    let unset_network_policy = map(
        rule! {
            UNSET ~ NETWORK ~ POLICY
        },
        |(_, _, _)| UserOptionItem::UnsetNetworkPolicy,
    );
    alt((
        value(UserOptionItem::TenantSetting(true), rule! { TENANTSETTING }),
        value(
            UserOptionItem::TenantSetting(false),
            rule! { NOTENANTSETTING },
        ),
        default_role_option,
        set_network_policy,
        unset_network_policy,
    ))(i)
}

pub fn user_identity(i: Input) -> IResult<UserIdentity> {
    map(
        rule! {
            #parameter_to_string ~ ( "@" ~  "'%'" )?
        },
        |(username, _)| {
            let hostname = "%".to_string();
            UserIdentity { username, hostname }
        },
    )(i)
}

pub fn auth_type(i: Input) -> IResult<AuthType> {
    alt((
        value(AuthType::NoPassword, rule! { NO_PASSWORD }),
        value(AuthType::Sha256Password, rule! { SHA256_PASSWORD }),
        value(AuthType::DoubleSha1Password, rule! { DOUBLE_SHA1_PASSWORD }),
        value(AuthType::JWT, rule! { JWT }),
    ))(i)
}

pub fn presign_action(i: Input) -> IResult<PresignAction> {
    alt((
        value(PresignAction::Download, rule! { DOWNLOAD }),
        value(PresignAction::Upload, rule! { UPLOAD }),
    ))(i)
}

pub fn presign_location(i: Input) -> IResult<PresignLocation> {
    map_res(
        rule! {
            #stage_location
        },
        |v| Ok(PresignLocation::StageLocation(v)),
    )(i)
}

pub fn presign_option(i: Input) -> IResult<PresignOption> {
    alt((
        map(rule! { EXPIRE ~ "=" ~ #literal_u64 }, |(_, _, v)| {
            PresignOption::Expire(v)
        }),
        map(
            rule! { CONTENT_TYPE ~ "=" ~ #literal_string },
            |(_, _, v)| PresignOption::ContentType(v),
        ),
    ))(i)
}

pub fn table_reference_only(i: Input) -> IResult<TableReference> {
    map(
        consumed(rule! {
            #dot_separated_idents_1_to_3
        }),
        |(span, (catalog, database, table))| TableReference::Table {
            span: transform_span(span.0),
            catalog,
            database,
            table,
            alias: None,
            travel_point: None,
            pivot: None,
            unpivot: None,
        },
    )(i)
}

pub fn update_expr(i: Input) -> IResult<UpdateExpr> {
    map(rule! { ( #ident ~ "=" ~ ^#expr ) }, |(name, _, expr)| {
        UpdateExpr { name, expr }
    })(i)
}

pub fn udf_arg_type(i: Input) -> IResult<TypeName> {
    let nullable = alt((
        value(true, rule! { NULL }),
        value(false, rule! { NOT ~ ^NULL }),
    ));
    map(
        rule! {
            #type_name ~ #nullable?
        },
        |(type_name, nullable)| match nullable {
            Some(false) => type_name,
            _ => type_name.wrap_nullable(),
        },
    )(i)
}

pub fn udf_definition(i: Input) -> IResult<UDFDefinition> {
    let lambda_udf = map(
        rule! {
            AS ~ "(" ~ #comma_separated_list0(ident) ~ ")"
            ~ "->" ~ #expr
        },
        |(_, _, parameters, _, _, definition)| UDFDefinition::LambdaUDF {
            parameters,
            definition: Box::new(definition),
        },
    );

    let udf_server = map(
        rule! {
            "(" ~ #comma_separated_list0(udf_arg_type) ~ ")"
            ~ RETURNS ~ #udf_arg_type
            ~ LANGUAGE ~ #ident
            ~ HANDLER ~ ^"=" ~ ^#literal_string
            ~ ADDRESS ~ ^"=" ~ ^#literal_string
        },
        |(_, arg_types, _, _, return_type, _, language, _, _, handler, _, _, address)| {
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                address,
                handler,
                language: language.to_string(),
            }
        },
    );

    rule!(
        #udf_server: "(<arg_type>, ...) RETURNS <return_type> LANGUAGE <language> HANDLER=<handler> ADDRESS=<udf_server_address>"
        | #lambda_udf: "AS (<parameter>, ...) -> <definition expr>"
    )(i)
}

pub fn merge_update_expr(i: Input) -> IResult<MergeUpdateExpr> {
    map(
        rule! { ( #dot_separated_idents_1_to_3 ~ "=" ~ ^#expr ) },
        |((catalog, table, name), _, expr)| MergeUpdateExpr {
            catalog,
            table,
            name,
            expr,
        },
    )(i)
}

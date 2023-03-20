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

use std::collections::BTreeMap;
use std::time::Duration;

use common_meta_app::principal::AuthType;
use common_meta_app::principal::FileFormatOptions;
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
use crate::parser::expr::subexpr;
use crate::parser::expr::*;
use crate::parser::query::*;
use crate::parser::stage::*;
use crate::parser::token::*;
use crate::rule;
use crate::util::*;
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
            EXPLAIN ~ ( AST | SYNTAX | PIPELINE | GRAPH | FRAGMENTS | RAW | MEMO )? ~ #statement
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

    let insert = map(
        rule! {
            INSERT ~ ( INTO | OVERWRITE ) ~ TABLE?
            ~ #period_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ #insert_source
        },
        |(_, overwrite, _, (catalog, database, table), opt_columns, source)| {
            Statement::Insert(InsertStmt {
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
            REPLACE ~ INTO?
            ~ #period_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ (ON ~ CONFLICT? ~ "(" ~ #comma_separated_list1(ident) ~ ")")
            ~ #insert_source
        },
        |(
            _,
            _,
            (catalog, database, table),
            opt_columns,
            (_, _, _, on_conflict_columns, _),
            source,
        )| {
            Statement::Replace(ReplaceStmt {
                catalog,
                database,
                table,
                on_conflict_columns,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                source,
            })
        },
    );

    let delete = map(
        rule! {
            DELETE ~ FROM ~ #table_reference_only
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, _, table_reference, opt_selection)| Statement::Delete {
            table_reference,
            selection: opt_selection.map(|(_, selection)| selection),
        },
    );

    let update = map(
        rule! {
            UPDATE ~ #table_reference_only
            ~ SET ~ ^#comma_separated_list1(update_expr)
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, table, _, update_list, opt_selection)| {
            Statement::Update(UpdateStmt {
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
            SET ~ (GLOBAL)? ~ #ident ~ "=" ~ #subexpr(0)
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
            SET ~ (DEFAULT)? ~ ROLE ~ #literal_string
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
            CREATE ~ CATALOG ~ ( IF ~ NOT ~ EXISTS )?
            ~ #ident
            ~ TYPE ~ "=" ~ #catalog_type
            ~ CONNECTION ~ "=" ~ #options
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
            DROP ~ CATALOG ~ ( IF ~ EXISTS )? ~ #ident
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
            SHOW ~ FULL? ~ ( DATABASES | SCHEMAS ) ~ ( ( FROM | IN) ~ ^#ident )? ~ #show_limit?
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
            SHOW ~ CREATE ~ ( DATABASE | SCHEMA ) ~ #period_separated_idents_1_to_2
        },
        |(_, _, _, (catalog, database))| {
            Statement::ShowCreateDatabase(ShowCreateDatabaseStmt { catalog, database })
        },
    );
    let create_database = map(
        rule! {
            CREATE ~ ( DATABASE | SCHEMA ) ~ ( IF ~ NOT ~ EXISTS )? ~ #period_separated_idents_1_to_2 ~ #create_database_option?
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
            DROP ~ ( DATABASE | SCHEMA ) ~ ( IF ~ EXISTS )? ~ #period_separated_idents_1_to_2
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
            UNDROP ~ DATABASE ~ #period_separated_idents_1_to_2
        },
        |(_, _, (catalog, database))| {
            Statement::UndropDatabase(UndropDatabaseStmt { catalog, database })
        },
    );

    let alter_database = map(
        rule! {
            ALTER ~ DATABASE ~ ( IF ~ EXISTS )? ~ #period_separated_idents_1_to_2 ~ #alter_database_action
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
            SHOW ~ FULL? ~ TABLES ~ HISTORY? ~ ( ( FROM | IN ) ~ #period_separated_idents_1_to_2 )? ~ #show_limit?
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
    let show_create_table = map(
        rule! {
            SHOW ~ CREATE ~ TABLE ~ #period_separated_idents_1_to_3
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
            ( DESC | DESCRIBE ) ~ #period_separated_idents_1_to_3
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
            SHOW ~ FIELDS ~ FROM ~ #period_separated_idents_1_to_3
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
    let create_table = map(
        rule! {
            CREATE ~ TRANSIENT? ~ TABLE ~ ( IF ~ NOT ~ EXISTS )?
            ~ #period_separated_idents_1_to_3
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
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ #period_separated_idents_1_to_3 ~ ( ALL )?
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
            UNDROP ~ TABLE ~ #period_separated_idents_1_to_3
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
            ALTER ~ TABLE ~ ( IF ~ EXISTS )? ~ #table_reference_only ~ #alter_table_action
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
            RENAME ~ TABLE ~ ( IF ~ EXISTS )? ~ #period_separated_idents_1_to_3 ~ TO ~ #period_separated_idents_1_to_3
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
            TRUNCATE ~ TABLE ~ #period_separated_idents_1_to_3 ~ PURGE?
        },
        |(_, _, (catalog, database, table), opt_purge)| {
            Statement::TruncateTable(TruncateTableStmt {
                catalog,
                database,
                table,
                purge: opt_purge.is_some(),
            })
        },
    );
    let optimize_table = map(
        rule! {
            OPTIMIZE ~ TABLE ~ #period_separated_idents_1_to_3 ~ #optimize_table_action
        },
        |(_, _, (catalog, database, table), action)| {
            Statement::OptimizeTable(OptimizeTableStmt {
                catalog,
                database,
                table,
                action,
            })
        },
    );
    let analyze_table = map(
        rule! {
            ANALYZE ~ TABLE ~ #period_separated_idents_1_to_3
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
            EXISTS ~ TABLE ~ #period_separated_idents_1_to_3
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
            CREATE ~ VIEW ~ ( IF ~ NOT ~ EXISTS )?
            ~ #period_separated_idents_1_to_3
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
            DROP ~ VIEW ~ ( IF ~ EXISTS )? ~ #period_separated_idents_1_to_3
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
            ~ #period_separated_idents_1_to_3
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
    let show_users = value(Statement::ShowUsers, rule! { SHOW ~ USERS });
    let create_user = map(
        rule! {
            CREATE ~ USER ~ ( IF ~ NOT ~ EXISTS )?
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
            DROP ~ USER ~ ( IF ~ EXISTS )? ~ #user_identity
        },
        |(_, _, opt_if_exists, user)| Statement::DropUser {
            if_exists: opt_if_exists.is_some(),
            user,
        },
    );
    let show_roles = value(Statement::ShowRoles, rule! { SHOW ~ ROLES });
    let create_role = map(
        rule! {
            CREATE ~ ROLE ~ ( IF ~ NOT ~ EXISTS )? ~ #literal_string
        },
        |(_, _, opt_if_not_exists, role_name)| Statement::CreateRole {
            if_not_exists: opt_if_not_exists.is_some(),
            role_name,
        },
    );
    let drop_role = map(
        rule! {
            DROP ~ ROLE ~ ( IF ~ EXISTS )? ~ #literal_string
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

    // stages
    let create_stage = map_res(
        rule! {
            CREATE ~ STAGE ~ ( IF ~ NOT ~ EXISTS )?
            ~ ( #stage_name )
            ~ ( URL ~ "=" ~ #uri_location)?
            ~ ( #file_format_clause )?
            ~ ( ON_ERROR ~ "=" ~ #ident)?
            ~ ( SIZE_LIMIT ~ "=" ~ #literal_u64)?
            ~ ( VALIDATION_MODE ~ "=" ~ #ident)?
            ~ ( (COMMENT | COMMENTS) ~ "=" ~ #literal_string)?
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
            pattern: opt_pattern.map(|v| v.2).unwrap_or_default(),
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
            DROP ~ STAGE ~ ( IF ~ EXISTS )? ~ #stage_name
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

    let copy_into = map(
        rule! {
            COPY
            ~ INTO ~ #copy_unit
            ~ FROM ~ #copy_unit
            ~ ( #copy_option )*
        },
        |(_, _, dst, _, src, opts)| {
            let mut copy_stmt = CopyStmt {
                src,
                dst,
                files: Default::default(),
                pattern: Default::default(),
                file_format: Default::default(),
                validation_mode: Default::default(),
                size_limit: Default::default(),
                max_file_size: Default::default(),
                split_size: Default::default(),
                single: Default::default(),
                purge: Default::default(),
                force: Default::default(),
                on_error: "abort".to_string(),
            };
            for opt in opts {
                copy_stmt.apply_option(opt);
            }
            Statement::Copy(copy_stmt)
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
    let create_share = map(
        rule! {
            CREATE ~ SHARE ~ (IF ~ NOT ~ EXISTS )? ~ #ident ~ ( COMMENT ~ "=" ~ #literal_string)?
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
            DROP ~ SHARE ~ (IF ~ EXISTS)? ~ #ident
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
            ALTER ~ SHARE ~ (IF ~ EXISTS )? ~ #ident ~ #alter_add_share_accounts ~ TENANTS ~ Eq ~ #comma_separated_list1(ident)
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
            CREATE ~ FILE ~ FORMAT ~ ( IF ~ NOT ~ EXISTS )?
            ~ #ident ~ #format_options
        },
        |(_, _, _, opt_if_not_exists, name, file_format_options)| {
            let file_format_options = FileFormatOptions::from_map(&file_format_options)
                .map_err(|_| ErrorKind::Other("invalid statement"))?;
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
        rule!(
            #insert : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #replace : "`REPLACE INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
        ),
        rule!(
            #set_variable : "`SET <variable> = <value>`"
            | #unset_variable : "`UNSET <variable>`"
        ),
        rule!(
            #show_tables : "`SHOW [FULL] TABLES [FROM <database>] [<show_limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #describe_table : "`DESCRIBE [<database>.]<table>`"
            | #show_fields : "`SHOW FIELDS FROM [<database>.]<table>`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show_limit>]`"
            | #create_table : "`CREATE TABLE [IF NOT EXISTS] [<database>.]<table> [<source>] [<table_options>]`"
            | #drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
            | #undrop_table : "`UNDROP TABLE [<database>.]<table>`"
            | #alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #truncate_table : "`TRUNCATE TABLE [<database>.]<table> [PURGE]`"
            | #optimize_table : "`OPTIMIZE TABLE [<database>.]<table> (ALL | PURGE | COMPACT [SEGMENT])`"
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
            #show_users : "`SHOW USERS`"
            | #create_user : "`CREATE USER [IF NOT EXISTS] '<username>'@'hostname' IDENTIFIED [WITH <auth_type>] [BY <password>] [WITH <user_option>, ...]`"
            | #alter_user : "`ALTER USER ('<username>'@'hostname' | USER()) [IDENTIFIED [WITH <auth_type>] [BY <password>]] [WITH <user_option>, ...]`"
            | #drop_user : "`DROP USER [IF EXISTS] '<username>'@'hostname'`"
            | #show_roles : "`SHOW ROLES`"
            | #create_role : "`CREATE ROLE [IF NOT EXISTS] '<role_name>']`"
            | #drop_role : "`DROP ROLE [IF EXISTS] '<role_name>'`"
            | #create_udf : "`CREATE FUNCTION [IF NOT EXISTS] <udf_name> (<parameter>, ...) -> <definition expr> [DESC = <description>]`"
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
        rule!(
            #copy_into: "`COPY
                INTO { internalStage | externalStage | externalLocation | [<database_name>.]<table_name> }
                FROM { internalStage | externalStage | externalLocation | [<database_name>.]<table_name> | ( <query> ) }
                [ FILE_FORMAT = ( { TYPE = { CSV | JSON | PARQUET } [ formatTypeOptions ] } ) ]
                [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
                [ PATTERN = '<regex_pattern>' ]
                [ VALIDATION_MODE = RETURN_ROWS ]
                [ copyOptions ]`"
        ),
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
        // share
        rule!(
            #create_share: "`CREATE SHARE [IF NOT EXISTS] <share_name> [ COMMENT = '<string_literal>' ]`"
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
    ));

    map(
        rule! {
            #statement_body ~ (FORMAT ~ #ident)? ~ ";"? ~ &EOI
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
           #file_format_clause ~ #rest_str
        },
        |(options, (_, start))| InsertSource::StreamingV2 {
            settings: options,
            start,
        },
    );
    let values = map(
        rule! {
            VALUES ~ #rest_str
        },
        |(_, (rest_str, _))| InsertSource::Values { rest_str },
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

pub fn rest_str(i: Input) -> IResult<(String, usize)> {
    // It's safe to unwrap because input must contain EOI.
    let first_token = i.0.first().unwrap();
    let last_token = i.0.last().unwrap();
    Ok((
        i.slice((i.len() - 1)..),
        (
            first_token.source[first_token.span.start..last_token.span.end].to_string(),
            first_token.span.start,
        ),
    ))
}

pub fn column_def(i: Input) -> IResult<ColumnDefinition> {
    #[derive(Clone)]
    enum ColumnConstraint {
        Nullable(bool),
        DefaultExpr(Box<Expr>),
    }

    let nullable = alt((
        value(ColumnConstraint::Nullable(true), rule! { NULL }),
        value(ColumnConstraint::Nullable(false), rule! { NOT ~ ^NULL }),
    ));
    let default_expr = map(
        rule! {
            DEFAULT ~ ^#subexpr(NOT_PREC)
        },
        |(_, default_expr)| ColumnConstraint::DefaultExpr(Box::new(default_expr)),
    );

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
            ~ ( #nullable | #default_expr )*
            ~ ( #comment )?
            : "`<column name> <type> [DEFAULT <default value>] [COMMENT '<comment>']`"
        },
        |(name, data_type, constraints, comment)| {
            let mut def = ColumnDefinition {
                name,
                data_type,
                default_expr: None,
                comment,
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::DefaultExpr(default_expr) => {
                        def.default_expr = Some(default_expr)
                    }
                    ColumnConstraint::Nullable(nullable) => {
                        if nullable {
                            def.data_type = def.data_type.wrap_nullable();
                        }
                    }
                }
            }
            def
        },
    )(i)
}

pub fn grant_source(i: Input) -> IResult<AccountMgrSource> {
    let role = map(
        rule! {
            ROLE ~ #literal_string
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
        value(UserPrivilegeType::Drop, rule! { DROP }),
        value(UserPrivilegeType::Alter, rule! { ALTER }),
        value(UserPrivilegeType::Super, rule! { SUPER }),
        value(UserPrivilegeType::CreateUser, rule! { CREATE ~ USER }),
        value(UserPrivilegeType::CreateRole, rule! { CREATE ~ ROLE }),
        value(UserPrivilegeType::Grant, rule! { GRANT }),
        value(UserPrivilegeType::CreateStage, rule! { CREATE ~ STAGE }),
        value(UserPrivilegeType::Set, rule! { SET }),
        value(UserPrivilegeType::Create, rule! { CREATE }),
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
            ROLE ~ #literal_string
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
            LIKE ~ #period_separated_idents_1_to_3
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

pub fn alter_table_action(i: Input) -> IResult<AlterTableAction> {
    let rename_table = map(
        rule! {
            RENAME ~ TO ~ #ident
        },
        |(_, _, new_table)| AlterTableAction::RenameTable { new_table },
    );
    let add_column = map(
        rule! {
            ADD ~ COLUMN ~ #column_def
        },
        |(_, _, column)| AlterTableAction::AddColumn { column },
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
            RECLUSTER ~ FINAL? ~ ( WHERE ~ ^#expr )?
        },
        |(_, opt_is_final, opt_selection)| AlterTableAction::ReclusterTable {
            is_final: opt_is_final.is_some(),
            selection: opt_selection.map(|(_, selection)| selection),
        },
    );

    let revert_table = map(
        rule! {
            FLASHBACK ~ TO ~ #travel_point
        },
        |(_, _, point)| AlterTableAction::RevertTo { point },
    );

    rule!(
        #rename_table
        | #add_column
        | #drop_column
        | #alter_table_cluster_key
        | #drop_table_cluster_key
        | #recluster_table
        | #revert_table
    )(i)
}

pub fn optimize_table_action(i: Input) -> IResult<OptimizeTableAction> {
    alt((
        value(OptimizeTableAction::All, rule! { ALL }),
        map(
            rule! { PURGE ~ (BEFORE ~ #travel_point)?},
            |(_, opt_travel_point)| OptimizeTableAction::Purge {
                before: opt_travel_point.map(|(_, p)| p),
            },
        ),
        map(
            rule! { COMPACT ~ (SEGMENT)? ~ ( LIMIT ~ ^#expr )?},
            |(_, opt_segment, opt_limit)| OptimizeTableAction::Compact {
                target: opt_segment.map_or(CompactTarget::Block, |_| CompactTarget::Segment),
                limit: opt_limit.map(|(_, limit)| limit),
            },
        ),
    ))(i)
}

pub fn kill_target(i: Input) -> IResult<KillTarget> {
    alt((
        value(KillTarget::Query, rule! { QUERY }),
        value(KillTarget::Connection, rule! { CONNECTION }),
    ))(i)
}

/// Parse input into `CopyUnit`
///
/// # Notes
///
/// It's required to parse stage location first. Or stage could be parsed as table.
pub fn copy_unit(i: Input) -> IResult<CopyUnit> {
    // Parse input like `@my_stage/path/to/dir`
    let stage_location = |i| {
        map_res(
            rule! {
                #stage_location
            },
            |v| Ok(CopyUnit::StageLocation(v)),
        )(i)
    };

    // Parse input like `mytable`
    let table = |i| {
        map(
            period_separated_idents_1_to_3,
            |(catalog, database, table)| CopyUnit::Table {
                catalog,
                database,
                table,
            },
        )(i)
    };

    // Parse input like `( SELECT * from mytable )`
    let query = |i| {
        map(parenthesized_query, |query| {
            CopyUnit::Query(Box::new(query))
        })(i)
    };

    // Parse input like `'s3://example/path/to/dir' CREDENTIALS = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
    let inner_uri_location = |i| {
        map_res(
            rule! {
                #uri_location
            },
            |v| Ok(CopyUnit::UriLocation(v)),
        )(i)
    };

    rule!(
       #stage_location: "@<stage_name> { <path> }"
        | #inner_uri_location: "'<protocol>://<name> {<path>} { CONNECTION = ({ AWS_ACCESS_KEY = 'aws_access_key' }) } '"
        | #table: "{ { <catalog>. } <database>. }<table>"
        | #query: "( <query> )"
    )(i)
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
           ( #ident_to_string ~ "=" ~ #parameter_to_string )*
        },
        |opts| BTreeMap::from_iter(opts.iter().map(|(k, _, v)| (k.to_lowercase(), v.clone()))),
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
            "DEFAULT_ROLE" ~ "=" ~ #literal_string
        },
        |(_, _, role)| UserOptionItem::DefaultRole(role),
    );
    alt((
        value(UserOptionItem::TenantSetting(true), rule! { TENANTSETTING }),
        value(
            UserOptionItem::TenantSetting(false),
            rule! { NOTENANTSETTING },
        ),
        default_role_option,
    ))(i)
}

pub fn user_identity(i: Input) -> IResult<UserIdentity> {
    map(
        rule! {
            #parameter_to_string ~ ( "@" ~ #literal_string )?
        },
        |(username, opt_hostname)| UserIdentity {
            username,
            hostname: opt_hostname
                .map(|(_, hostname)| hostname)
                .unwrap_or_else(|| "%".to_string()),
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

pub fn copy_option(i: Input) -> IResult<CopyOption> {
    alt((
        map(
            rule! { FILES ~ "=" ~ "(" ~ #comma_separated_list0(literal_string) ~ ")" },
            |(_, _, _, files, _)| CopyOption::Files(files),
        ),
        map(
            rule! { PATTERN ~ "=" ~ #literal_string },
            |(_, _, pattern)| CopyOption::Pattern(pattern),
        ),
        map(rule! { #file_format_clause }, |options| {
            CopyOption::FileFormat(options)
        }),
        map(
            rule! { VALIDATION_MODE ~ "=" ~ #literal_string },
            |(_, _, validation_mode)| CopyOption::ValidationMode(validation_mode),
        ),
        map(
            rule! { SIZE_LIMIT ~ "=" ~ #literal_u64 },
            |(_, _, size_limit)| CopyOption::SizeLimit(size_limit as usize),
        ),
        map(
            rule! { MAX_FILE_SIZE ~ "=" ~ #literal_u64 },
            |(_, _, max_file_size)| CopyOption::MaxFileSize(max_file_size as usize),
        ),
        map(
            rule! { SPLIT_SIZE ~ "=" ~ #literal_u64 },
            |(_, _, split_size)| CopyOption::SplitSize(split_size as usize),
        ),
        map(rule! { SINGLE ~ "=" ~ #literal_bool }, |(_, _, single)| {
            CopyOption::Single(single)
        }),
        map(rule! { PURGE ~ "=" ~ #literal_bool }, |(_, _, purge)| {
            CopyOption::Purge(purge)
        }),
        map(rule! { FORCE ~ "=" ~ #literal_bool }, |(_, _, force)| {
            CopyOption::Force(force)
        }),
        map(rule! {ON_ERROR ~ "=" ~ #ident}, |(_, _, on_error)| {
            CopyOption::OnError(on_error.to_string())
        }),
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
            #period_separated_idents_1_to_3
        }),
        |(span, (catalog, database, table))| TableReference::Table {
            span: transform_span(span.0),
            catalog,
            database,
            table,
            alias: None,
            travel_point: None,
            pivot: None, // TODO(Sky)
        },
    )(i)
}

pub fn update_expr(i: Input) -> IResult<UpdateExpr> {
    map(rule! { ( #ident ~ "=" ~ ^#expr ) }, |(name, _, expr)| {
        UpdateExpr { name, expr }
    })(i)
}

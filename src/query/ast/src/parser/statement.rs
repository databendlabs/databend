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

use educe::Educe;
use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::not;
use nom::combinator::value;
use nom::Slice;
use nom_rule::rule;

use super::sequence::sequence;
use crate::ast::*;
use crate::parser::common::*;
use crate::parser::copy::copy_into;
use crate::parser::copy::copy_into_table;
use crate::parser::data_mask::data_mask_policy;
use crate::parser::dynamic_table::dynamic_table;
use crate::parser::expr::subexpr;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::query::*;
use crate::parser::stage::*;
use crate::parser::stream::stream_table;
use crate::parser::token::*;
use crate::parser::Error;
use crate::parser::ErrorKind;
use crate::span::merge_span;

pub enum ShowGrantOption {
    PrincipalIdentity(PrincipalIdentity),
    GrantObjectName(GrantObjectName),
}

// (tenant, share name, endpoint name)
pub type ShareDatabaseParams = (ShareNameIdent, Identifier);

#[derive(Clone)]
pub enum CreateDatabaseOption {
    DatabaseEngine(DatabaseEngine),
}

pub fn statement_body(i: Input) -> IResult<Statement> {
    let explain = map_res(
        rule! {
            EXPLAIN ~ ( "(" ~ #comma_separated_list1(explain_option) ~ ")" )? ~ ( AST | SYNTAX | PIPELINE | JOIN | GRAPH | FRAGMENTS | RAW | OPTIMIZED | MEMO | DECORRELATED)? ~ #statement
        },
        |(_, options, opt_kind, statement)| {
            Ok(Statement::Explain {
                kind: match opt_kind.map(|token| token.kind) {
                    Some(TokenKind::SYNTAX) | Some(TokenKind::AST) => {
                        let pretty_stmt = statement.stmt.to_string();
                        ExplainKind::Syntax(pretty_stmt)
                    }
                    Some(TokenKind::PIPELINE) => ExplainKind::Pipeline,
                    Some(TokenKind::JOIN) => ExplainKind::Join,
                    Some(TokenKind::GRAPH) => ExplainKind::Graph,
                    Some(TokenKind::FRAGMENTS) => ExplainKind::Fragments,
                    Some(TokenKind::RAW) => ExplainKind::Raw,
                    Some(TokenKind::OPTIMIZED) => ExplainKind::Optimized,
                    Some(TokenKind::DECORRELATED) => ExplainKind::Decorrelated,
                    Some(TokenKind::MEMO) => ExplainKind::Memo("".to_string()),
                    Some(TokenKind::GRAPHICAL) => ExplainKind::Graphical,
                    None => ExplainKind::Plan,
                    _ => unreachable!(),
                },
                options: options
                    .map(|(a, opts, b)| (merge_span(Some(a.span), Some(b.span)), opts))
                    .unwrap_or_default(),
                query: Box::new(statement.stmt),
            })
        },
    );

    let query_setting = map_res(
        rule! {
            SETTINGS ~ #query_statement_setting? ~ #statement_body
        },
        |(_, opt_settings, statement)| {
            Ok(Statement::StatementWithSettings {
                settings: opt_settings,
                stmt: Box::new(statement),
            })
        },
    );
    let explain_analyze = map(
        rule! {
            EXPLAIN ~ ANALYZE ~ (PARTIAL|GRAPHICAL)? ~ #statement
        },
        |(_, _, opt_partial_or_graphical, statement)| {
            let (partial, graphical) = match opt_partial_or_graphical {
                Some(Token {
                    kind: TokenKind::PARTIAL,
                    ..
                }) => (true, false),
                Some(Token {
                    kind: TokenKind::GRAPHICAL,
                    ..
                }) => (false, true),
                _ => (false, false),
            };
            Statement::ExplainAnalyze {
                partial,
                graphical,
                query: Box::new(statement.stmt),
            }
        },
    );

    let create_task = map(
        rule! {
            CREATE ~ TASK ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ #create_task_option*
            ~ #set_table_option?
            ~ AS ~ #task_sql_block
        },
        |(_, _, opt_if_not_exists, task, create_task_opts, session_opts, _, sql)| {
            let session_opts = session_opts.unwrap_or_default();
            let mut stmt = CreateTaskStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: task.to_string(),
                warehouse: None,
                schedule_opts: None,
                suspend_task_after_num_failures: None,
                comments: None,
                after: vec![],
                error_integration: None,
                when_condition: None,
                sql,
                session_parameters: session_opts,
            };
            for opt in create_task_opts {
                stmt.apply_opt(opt);
            }
            Statement::CreateTask(stmt)
        },
    );

    let alter_task = map(
        rule! {
            ALTER ~ TASK ~ ( IF ~ ^EXISTS )?
            ~ #ident ~ #alter_task_option
        },
        |(_, _, opt_if_exists, task, options)| {
            Statement::AlterTask(AlterTaskStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
                options,
            })
        },
    );

    let drop_task = map(
        rule! {
            DROP ~ TASK ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, opt_if_exists, task)| {
            Statement::DropTask(DropTaskStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
            })
        },
    );
    let show_tasks = map(
        rule! {
            SHOW ~ TASKS ~ #show_limit?
        },
        |(_, _, limit)| Statement::ShowTasks(ShowTasksStmt { limit }),
    );

    let execute_task = map(
        rule! {
            EXECUTE ~ TASK ~ #ident
        },
        |(_, _, task)| {
            Statement::ExecuteTask(ExecuteTaskStmt {
                name: task.to_string(),
            })
        },
    );

    let desc_task = map(
        rule! {
            ( DESC | DESCRIBE ) ~ TASK ~ #ident
        },
        |(_, _, task)| {
            Statement::DescribeTask(DescribeTaskStmt {
                name: task.to_string(),
            })
        },
    );

    let merge = map(
        rule! {
            MERGE ~ #hint?
            ~ INTO ~ #dot_separated_idents_1_to_3 ~ #table_alias?
            ~ USING ~ #mutation_source
            ~ ON ~ #expr ~ (#match_clause | #unmatch_clause)*
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
            #with? ~ DELETE ~ #hint? ~ FROM ~ #table_reference_with_alias ~ ( WHERE ~ ^#expr )?
        },
        |(with, _, hints, _, table, opt_selection)| {
            Statement::Delete(DeleteStmt {
                hints,
                table,
                selection: opt_selection.map(|(_, selection)| selection),
                with,
            })
        },
    );

    let update = map(
        rule! {
            #with? ~ UPDATE ~ #hint? ~ #dot_separated_idents_1_to_3 ~ #table_alias?
            ~ SET ~ ^#comma_separated_list1(mutation_update_expr)
            ~ ( FROM ~ #mutation_source )?
            ~ ( WHERE ~ ^#expr )?
        },
        |(
            with,
            _,
            hints,
            (catalog, database, table),
            table_alias,
            _,
            update_list,
            from,
            opt_selection,
        )| {
            Statement::Update(UpdateStmt {
                hints,
                catalog,
                database,
                table,
                table_alias,
                update_list,
                from: from.map(|(_, table)| table),
                selection: opt_selection.map(|(_, selection)| selection),
                with,
            })
        },
    );

    let show_settings = map(
        rule! {
            SHOW ~ SETTINGS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowSettings { show_options },
    );
    let show_variables = map(
        rule! {
            SHOW ~ VARIABLES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowVariables { show_options },
    );
    let show_stages = map(
        rule! {
            SHOW ~ STAGES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowStages { show_options },
    );
    let show_process_list = map(
        rule! {
            SHOW ~ PROCESSLIST ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowProcessList { show_options },
    );
    let show_metrics = map(
        rule! {
            SHOW ~ METRICS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowMetrics { show_options },
    );
    let show_engines = map(
        rule! {
            SHOW ~ ENGINES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowEngines { show_options },
    );
    let show_functions = map(
        rule! {
            SHOW ~ FUNCTIONS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowFunctions { show_options },
    );
    let show_user_functions = map(
        rule! {
            SHOW ~ USER ~ FUNCTIONS ~ #show_options?
        },
        |(_, _, _, show_options)| Statement::ShowUserFunctions { show_options },
    );
    let show_table_functions = map(
        rule! {
            SHOW ~ TABLE_FUNCTIONS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowTableFunctions { show_options },
    );
    let show_indexes = map(
        rule! {
            SHOW ~ INDEXES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowIndexes { show_options },
    );
    let show_locks = map(
        rule! {
            SHOW ~ LOCKS ~ ( IN ~ ^ACCOUNT )? ~ #limit_where?
        },
        |(_, _, opt_in_account, limit)| {
            Statement::ShowLocks(ShowLocksStmt {
                in_account: opt_in_account.is_some(),
                limit,
            })
        },
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

    let set_priority = map(
        rule! {
            SET ~ PRIORITY ~  #priority  ~ #parameter_to_string
        },
        |(_, _, priority, object_id)| Statement::SetPriority {
            object_id,
            priority,
        },
    );

    let unset_stmt = map(
        rule! {
            UNSET ~ #set_type ~ #unset_source
        },
        |(_, unset_type, identifiers)| Statement::UnSetStmt {
            settings: Settings {
                set_type: unset_type,
                identifiers,
                values: SetValues::None,
            },
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

    let set_secondary_roles = map(
        rule! {
            SET ~ SECONDARY ~ ROLES ~ (ALL | NONE)
        },
        |(_, _, _, token)| {
            let option = match token.kind {
                TokenKind::ALL => SecondaryRolesOption::All,
                TokenKind::NONE => SecondaryRolesOption::None,
                _ => unreachable!(),
            };
            Statement::SetSecondaryRoles { option }
        },
    );

    let set_stmt = alt((
        map(
            rule! {
                SET ~ #set_type ~ #ident ~ "=" ~ #subexpr(0)
            },
            |(_, set_type, var, _, value)| Statement::SetStmt {
                settings: Settings {
                    set_type,
                    identifiers: vec![var],
                    values: SetValues::Expr(vec![Box::new(value)]),
                },
            },
        ),
        map_res(
            rule! {
                SET ~ #set_type ~ "(" ~ #comma_separated_list0(ident) ~ ")" ~ "="
                ~ "(" ~ #comma_separated_list0(subexpr(0)) ~ ")"
            },
            |(_, set_type, _, ids, _, _, _, values, _)| {
                if ids.len() == values.len() {
                    Ok(Statement::SetStmt {
                        settings: Settings {
                            set_type,
                            identifiers: ids,
                            values: SetValues::Expr(values.into_iter().map(|x| x.into()).collect()),
                        },
                    })
                } else {
                    Err(nom::Err::Failure(ErrorKind::Other(
                        "inconsistent number of variables and values",
                    )))
                }
            },
        ),
        map(
            rule! {
                SET ~ #set_type ~ #ident ~ "=" ~ #query
            },
            |(_, set_type, var, _, query)| Statement::SetStmt {
                settings: Settings {
                    set_type,
                    identifiers: vec![var],
                    values: SetValues::Query(Box::new(query)),
                },
            },
        ),
        map(
            rule! {
                SET ~ #set_type ~ "(" ~ #comma_separated_list0(ident) ~ ")" ~ "=" ~ #query
            },
            |(_, set_type, _, vars, _, _, query)| Statement::SetStmt {
                settings: Settings {
                    set_type,
                    identifiers: vars,
                    values: SetValues::Query(Box::new(query)),
                },
            },
        ),
    ));

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
    let use_catalog = map(
        rule! {
            USE ~ CATALOG ~ #ident
        },
        |(_, _, catalog)| Statement::UseCatalog { catalog },
    );

    let show_online_nodes = map(
        rule! {
            SHOW ~ ONLINE ~ NODES
        },
        |(_, _, _)| Statement::ShowOnlineNodes(ShowOnlineNodesStmt {}),
    );

    let show_warehouses = map(
        rule! {
            SHOW ~ WAREHOUSES
        },
        |(_, _)| Statement::ShowWarehouses(ShowWarehousesStmt {}),
    );

    let use_warehouse = map(
        rule! {
            USE ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::UseWarehouse(UseWarehouseStmt { warehouse }),
    );

    let create_warehouse = map(
        rule! {
            CREATE ~ WAREHOUSE ~ #ident ~ ("(" ~ #assign_nodes_list ~ ")")? ~ (WITH ~ #warehouse_cluster_option)?
        },
        |(_, _, warehouse, nodes, options)| {
            Statement::CreateWarehouse(CreateWarehouseStmt {
                warehouse,
                node_list: nodes.map(|(_, nodes, _)| nodes).unwrap_or_else(Vec::new),
                options: options.map(|(_, x)| x).unwrap_or_else(BTreeMap::new),
            })
        },
    );

    let drop_warehouse = map(
        rule! {
            DROP ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::DropWarehouse(DropWarehouseStmt { warehouse }),
    );

    let rename_warehouse = map(
        rule! {
            RENAME ~ WAREHOUSE ~ #ident ~ TO ~ #ident
        },
        |(_, _, warehouse, _, new_warehouse)| {
            Statement::RenameWarehouse(RenameWarehouseStmt {
                warehouse,
                new_warehouse,
            })
        },
    );

    let resume_warehouse = map(
        rule! {
            RESUME ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::ResumeWarehouse(ResumeWarehouseStmt { warehouse }),
    );

    let suspend_warehouse = map(
        rule! {
            SUSPEND ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::SuspendWarehouse(SuspendWarehouseStmt { warehouse }),
    );

    let inspect_warehouse = map(
        rule! {
            INSPECT ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::InspectWarehouse(InspectWarehouseStmt { warehouse }),
    );

    let add_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ ADD ~ CLUSTER ~ #ident ~ ("(" ~ #assign_nodes_list ~ ")")? ~ (WITH ~ #warehouse_cluster_option)?
        },
        |(_, _, warehouse, _, _, cluster, nodes, options)| {
            Statement::AddWarehouseCluster(AddWarehouseClusterStmt {
                warehouse,
                cluster,
                node_list: nodes.map(|(_, nodes, _)| nodes).unwrap_or_else(Vec::new),
                options: options.map(|(_, x)| x).unwrap_or_else(BTreeMap::new),
            })
        },
    );

    let drop_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ DROP ~ CLUSTER ~ #ident
        },
        |(_, _, warehouse, _, _, cluster)| {
            Statement::DropWarehouseCluster(DropWarehouseClusterStmt { warehouse, cluster })
        },
    );

    let rename_warehouse_cluster = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ RENAME ~ CLUSTER ~ #ident ~ TO ~ #ident
        },
        |(_, _, warehouse, _, _, cluster, _, new_cluster)| {
            Statement::RenameWarehouseCluster(RenameWarehouseClusterStmt {
                warehouse,
                cluster,
                new_cluster,
            })
        },
    );

    let assign_warehouse_nodes = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ ASSIGN ~ NODES ~ "(" ~ #assign_warehouse_nodes_list ~ ")"
        },
        |(_, _, warehouse, _, _, _, nodes, _)| {
            Statement::AssignWarehouseNodes(AssignWarehouseNodesStmt {
                warehouse,
                node_list: nodes,
            })
        },
    );

    let unassign_warehouse_nodes = map(
        rule! {
            ALTER ~ WAREHOUSE ~ #ident ~ UNASSIGN ~ NODES ~ "(" ~ #unassign_warehouse_nodes_list ~ ")"
        },
        |(_, _, warehouse, _, _, _, nodes, _)| {
            Statement::UnassignWarehouseNodes(UnassignWarehouseNodesStmt {
                warehouse,
                node_list: nodes,
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

    let show_drop_databases = map(
        rule! {
            SHOW ~ DROP ~ ( DATABASES | DATABASES ) ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_catalog, limit)| {
            Statement::ShowDropDatabases(ShowDropDatabasesStmt {
                catalog: opt_catalog.map(|(_, catalog)| catalog),
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

    let create_database = map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ( DATABASE | SCHEMA )
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #database_ref
            ~ #create_database_option?
        },
        |(_, opt_or_replace, _, opt_if_not_exists, database, create_database_option)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;

            let statement = match create_database_option {
                Some(CreateDatabaseOption::DatabaseEngine(engine)) => {
                    Statement::CreateDatabase(CreateDatabaseStmt {
                        create_option,
                        database,
                        engine: Some(engine),
                        options: vec![],
                    })
                }
                None => Statement::CreateDatabase(CreateDatabaseStmt {
                    create_option,
                    database,
                    engine: None,
                    options: vec![],
                }),
            };

            Ok(statement)
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
            SHOW
            ~ FULL? ~ COLUMNS
            ~ ( FROM | IN ) ~ #ident
            ~ (( FROM | IN ) ~ ^#dot_separated_idents_1_to_2)?
            ~ #show_limit?
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
            SHOW ~ CREATE ~ TABLE ~ #dot_separated_idents_1_to_3 ~ ( WITH ~ ^QUOTED_IDENTIFIERS )?
        },
        |(_, _, _, (catalog, database, table), comment_opt)| {
            Statement::ShowCreateTable(ShowCreateTableStmt {
                catalog,
                database,
                table,
                with_quoted_ident: comment_opt.is_some(),
            })
        },
    );
    let describe_table = map(
        rule! {
            ( DESC | DESCRIBE ) ~ TABLE? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, table))| {
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
            SHOW ~ DROP ~ ( TABLES | TABLE ) ~ ( FROM ~ ^#ident )? ~ #show_limit?
        },
        |(_, _, _, opt_database, limit)| {
            Statement::ShowDropTables(ShowDropTablesStmt {
                database: opt_database.map(|(_, database)| database),
                limit,
            })
        },
    );

    let attach_table = map(
        rule! {
            ATTACH ~ TABLE ~ #dot_separated_idents_1_to_3 ~ ("(" ~ #comma_separated_list1(ident) ~ ")")? ~ #uri_location
        },
        |(_, _, (catalog, database, table), columns_opt, uri_location)| {
            let columns_opt = columns_opt.map(|(_, v, _)| v);
            Statement::AttachTable(AttachTableStmt {
                catalog,
                database,
                table,
                columns_opt,
                uri_location,
            })
        },
    );
    let create_table = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ (TEMP| TEMPORARY|TRANSIENT)? ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ ( #engine )?
            ~ ( #uri_location )?
            ~ ( CLUSTER ~ ^BY ~ ( #cluster_type )? ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" )?
            ~ ( #table_option )?
            ~ ( AS ~ ^#query )?
        },
        |(
            _,
            opt_or_replace,
            opt_type,
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
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let table_type = match opt_type.map(|t| t.kind) {
                None => TableType::Normal,
                Some(TRANSIENT) => TableType::Transient,
                Some(TEMP) | Some(TEMPORARY) => TableType::Temporary,
                _ => unreachable!(),
            };
            Ok(Statement::CreateTable(CreateTableStmt {
                create_option,
                catalog,
                database,
                table,
                source,
                engine,
                uri_location,
                cluster_by: opt_cluster_by.map(|(_, _, typ, _, exprs, _)| ClusterOption {
                    cluster_type: typ.unwrap_or(ClusterType::Linear),
                    cluster_exprs: exprs,
                }),
                table_options: opt_table_options.unwrap_or_default(),
                as_query: opt_as_query.map(|(_, query)| Box::new(query)),
                table_type,
            }))
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
    let vacuum_temp_files = map(
        rule! {
            VACUUM ~ TEMPORARY ~ FILES ~ (RETAIN ~ #literal_duration)? ~ (LIMIT ~ #literal_u64)?
        },
        |(_, _, _, retain, opt_limit)| {
            Statement::VacuumTemporaryFiles(VacuumTemporaryFiles {
                limit: opt_limit.map(|(_, limit)| limit),
                retain: retain.map(|(_, reatin)| reatin),
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
            VACUUM ~ DROP ~ TABLE ~ (FROM ~ ^#dot_separated_idents_1_to_2)? ~ #vacuum_drop_table_option
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

    // DICTIONARY
    let create_dictionary = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ DICTIONARY ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ "(" ~ ^#comma_separated_list1(column_def) ~ ^")"
            ~ PRIMARY ~ ^KEY  ~ ^#comma_separated_list1(ident)
            ~ ^SOURCE ~ ^"(" ~ ^#ident ~ ^"("
            ~ ( #table_option )?
            ~ ^")" ~ ^")"
            ~ ( COMMENT ~ ^#literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (catalog, database, dictionary_name),
            _,
            columns,
            _,
            _,
            _,
            primary_keys,
            _,
            _,
            source_name,
            _,
            opt_source_options,
            _,
            _,
            opt_comment,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateDictionary(CreateDictionaryStmt {
                create_option,
                catalog,
                database,
                dictionary_name,
                columns,
                primary_keys,
                source_name,
                source_options: opt_source_options.unwrap_or_default(),
                comment: opt_comment.map(|(_, comment)| comment),
            }))
        },
    );
    let drop_dictionary = map(
        rule! {
            DROP ~ DICTIONARY ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, dictionary_name))| {
            Statement::DropDictionary(DropDictionaryStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                dictionary_name,
            })
        },
    );
    let show_dictionaries = map(
        rule! {
            SHOW ~ DICTIONARIES ~ ((FROM|IN) ~ #ident)? ~ #show_limit?
        },
        |(_, _, db, limit)| {
            let database = match db {
                Some((_, d)) => Some(d),
                _ => None,
            };
            Statement::ShowDictionaries(ShowDictionariesStmt { database, limit })
        },
    );
    let show_create_dictionary = map(
        rule! {
            SHOW ~ CREATE ~ DICTIONARY ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, (catalog, database, dictionary_name))| {
            Statement::ShowCreateDictionary(ShowCreateDictionaryStmt {
                catalog,
                database,
                dictionary_name,
            })
        },
    );
    let rename_dictionary = map(
        rule! {
            RENAME ~ DICTIONARY ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3 ~ TO ~ #dot_separated_idents_1_to_3
        },
        |(
            _,
            _,
            opt_if_exists,
            (catalog, database, dictionary),
            _,
            (new_catalog, new_database, new_dictionary),
        )| {
            Statement::RenameDictionary(RenameDictionaryStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                dictionary,
                new_catalog,
                new_database,
                new_dictionary,
            })
        },
    );

    let create_view = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ VIEW ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ AS ~ #query
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (catalog, database, view),
            opt_columns,
            _,
            query,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateView(CreateViewStmt {
                create_option,
                catalog,
                database,
                view,
                columns: opt_columns
                    .map(|(_, columns, _)| columns)
                    .unwrap_or_default(),
                query: Box::new(query),
            }))
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
    let show_views = map(
        rule! {
            SHOW ~ FULL? ~ VIEWS ~ HISTORY? ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, opt_history, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowViews(ShowViewsStmt {
                catalog,
                database,
                full: opt_full.is_some(),
                limit,
                with_history: opt_history.is_some(),
            })
        },
    );
    let describe_view = map(
        rule! {
            ( DESC | DESCRIBE ) ~ VIEW ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, view))| {
            Statement::DescribeView(DescribeViewStmt {
                catalog,
                database,
                view,
            })
        },
    );

    let create_index = map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ AGGREGATING ~ INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ AS ~ #query
        },
        |(_, opt_or_replace, opt_async, _, _, opt_if_not_exists, index_name, _, query)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateIndex(CreateIndexStmt {
                index_type: TableIndexType::Aggregating,
                create_option,
                index_name,
                query: Box::new(query),
                sync_creation: opt_async.is_none(),
            }))
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

    let create_inverted_index = map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ ASYNC?
            ~ INVERTED ~ INDEX
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
            ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
            ~ ( #table_option )?
        },
        |(
            _,
            opt_or_replace,
            opt_async,
            _,
            _,
            opt_if_not_exists,
            index_name,
            _,
            (catalog, database, table),
            _,
            columns,
            _,
            opt_index_options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateInvertedIndex(CreateInvertedIndexStmt {
                create_option,
                index_name,
                catalog,
                database,
                table,
                columns,
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            }))
        },
    );

    let drop_inverted_index = map(
        rule! {
            DROP ~ INVERTED ~ INDEX ~ ( IF ~ ^EXISTS )? ~ #ident
            ~ ON ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, opt_if_exists, index_name, _, (catalog, database, table))| {
            Statement::DropInvertedIndex(DropInvertedIndexStmt {
                if_exists: opt_if_exists.is_some(),
                index_name,
                catalog,
                database,
                table,
            })
        },
    );

    let refresh_inverted_index = map(
        rule! {
            REFRESH ~ INVERTED ~ INDEX ~ #ident ~ ON ~ #dot_separated_idents_1_to_3 ~ ( LIMIT ~ #literal_u64 )?
        },
        |(_, _, _, index_name, _, (catalog, database, table), opt_limit)| {
            Statement::RefreshInvertedIndex(RefreshInvertedIndexStmt {
                index_name,
                catalog,
                database,
                table,
                limit: opt_limit.map(|(_, limit)| limit),
            })
        },
    );

    let create_virtual_column = map_res(
        rule! {
            CREATE
            ~ ( OR ~ ^REPLACE )?
            ~ VIRTUAL ~ COLUMN
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ ^"(" ~ ^#comma_separated_list1(virtual_column) ~ ^")"
            ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(
            _,
            opt_or_replace,
            _,
            _,
            opt_if_not_exists,
            _,
            virtual_columns,
            _,
            _,
            (catalog, database, table),
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateVirtualColumn(CreateVirtualColumnStmt {
                create_option,
                catalog,
                database,
                table,
                virtual_columns,
            }))
        },
    );

    let alter_virtual_column = map(
        rule! {
            ALTER ~ VIRTUAL ~ COLUMN ~ ( IF ~ ^EXISTS )? ~ ^"(" ~ ^#comma_separated_list1(virtual_column) ~ ^")" ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, opt_if_exists, _, virtual_columns, _, _, (catalog, database, table))| {
            Statement::AlterVirtualColumn(AlterVirtualColumnStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                table,
                virtual_columns,
            })
        },
    );

    let drop_virtual_column = map(
        rule! {
            DROP ~ VIRTUAL ~ COLUMN ~ ( IF ~ ^EXISTS )? ~ FOR ~ #dot_separated_idents_1_to_3
        },
        |(_, _, _, opt_if_exists, _, (catalog, database, table))| {
            Statement::DropVirtualColumn(DropVirtualColumnStmt {
                if_exists: opt_if_exists.is_some(),
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

    let show_virtual_columns = map(
        rule! {
            SHOW ~ VIRTUAL ~ COLUMNS ~ ( ( FROM | IN ) ~ #ident )? ~ ( ( FROM | IN ) ~ ^#dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, _, _, opt_table, opt_db, limit)| {
            let table = opt_table.map(|(_, table)| table);
            let (catalog, database) = match opt_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowVirtualColumns(ShowVirtualColumnsStmt {
                catalog,
                database,
                table,
                limit,
            })
        },
    );

    let show_users = map(
        rule! {
            SHOW ~ USERS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowUsers { show_options },
    );

    let describe_user = map(
        rule! {
            ( DESC | DESCRIBE ) ~ USER ~ ^#user_identity
        },
        |(_, _, user)| Statement::DescribeUser { user },
    );
    let create_user = map_res(
        rule! {
            CREATE ~  ( OR ~ ^REPLACE )? ~ USER ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #user_identity
            ~ IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )?
            ~ ( WITH ~ ^#comma_separated_list1(user_option))?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            user,
            _,
            opt_auth_type,
            opt_password,
            opt_user_option,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateUser(CreateUserStmt {
                create_option,
                user,
                auth_option: AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                },
                user_options: opt_user_option
                    .map(|(_, user_options)| user_options)
                    .unwrap_or_default(),
            }))
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
    let show_roles = map(
        rule! {
            SHOW ~ ROLES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowRoles { show_options },
    );
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
    let grant_ownership = map(
        rule! {
            GRANT ~ OWNERSHIP ~ ON ~ #grant_ownership_level  ~ TO ~ ROLE ~ #role_name
        },
        |(_, _, _, level, _, _, role_name)| {
            Statement::Grant(GrantStmt {
                source: AccountMgrSource::Privs {
                    privileges: vec![UserPrivilegeType::Ownership],
                    level,
                },
                principal: PrincipalIdentity::Role(role_name),
            })
        },
    );
    let show_grants = map(
        rule! {
            SHOW ~ GRANTS ~ #show_grant_option? ~ ^#show_options?
        },
        |(_, _, show_grant_option, opt_limit)| match show_grant_option {
            Some(ShowGrantOption::PrincipalIdentity(principal)) => Statement::ShowGrants {
                principal: Some(principal),
                show_options: opt_limit,
            },
            None => Statement::ShowGrants {
                principal: None,
                show_options: opt_limit,
            },
            Some(ShowGrantOption::GrantObjectName(object)) => {
                Statement::ShowObjectPrivileges(ShowObjectPrivilegesStmt {
                    object,
                    show_option: opt_limit,
                })
            }
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
    let create_udf = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ FUNCTION ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #udf_definition
            ~ ( DESC ~ ^"=" ~ ^#literal_string )?
        },
        |(_, opt_or_replace, _, opt_if_not_exists, udf_name, definition, opt_description)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateUDF(CreateUDFStmt {
                create_option,
                udf_name,
                description: opt_description.map(|(_, _, description)| description),
                definition,
            }))
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
            CREATE ~ ( OR ~ ^REPLACE )? ~ STAGE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ ( #stage_name )
            ~ ( (URL ~ ^"=")? ~ #uri_location )?
            ~ ( #file_format_clause )?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            stage,
            url_opt,
            file_format_opt,
            comment_opt,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateStage(CreateStageStmt {
                create_option,
                stage_name: stage.to_string(),
                location: url_opt.map(|(_, location)| location),
                file_format_options: file_format_opt.unwrap_or_default(),
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

    // connections
    let connection_opt = connection_opt("=");
    let create_connection = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ CONNECTION ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ STORAGE_TYPE ~ "=" ~  #literal_string ~ #connection_opt*
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            connection_name,
            _,
            _,
            storage_type,
            options,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let options =
                BTreeMap::from_iter(options.iter().map(|(k, v)| (k.to_lowercase(), v.clone())));
            Ok(Statement::CreateConnection(CreateConnectionStmt {
                create_option,
                name: connection_name,
                storage_type,
                storage_params: options,
            }))
        },
    );

    let drop_connection = map(
        rule! {
            DROP ~ CONNECTION ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, connection_name)| {
            Statement::DropConnection(DropConnectionStmt {
                if_exists: opt_if_exists.is_some(),
                name: connection_name,
            })
        },
    );

    let desc_connection = map(
        rule! {
            (DESC | DESCRIBE) ~ CONNECTION ~ #ident
        },
        |(_, _, name)| Statement::DescribeConnection(DescribeConnectionStmt { name }),
    );

    let show_connections = map(
        rule! {
            SHOW ~ CONNECTIONS
        },
        |(_, _)| Statement::ShowConnections(ShowConnectionsStmt {}),
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

    let create_file_format = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ FILE ~ FORMAT ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident ~ #format_options
        },
        |(_, opt_or_replace, _, _, opt_if_not_exists, name, file_format_options)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateFileFormat {
                create_option,
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
    let create_data_mask_policy = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ MASKING ~ POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ #data_mask_policy
        },
        |(_, opt_or_replace, _, _, opt_if_not_exists, name, policy)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let stmt = CreateDatamaskPolicyStmt {
                create_option,
                name: name.to_string(),
                policy,
            };
            Ok(Statement::CreateDatamaskPolicy(stmt))
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

    let create_network_policy = map_res(
        rule! {
            CREATE ~  ( OR ~ ^REPLACE )? ~ NETWORK ~ ^POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ ^#ident
             ~ ALLOWED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")"
             ~ ( BLOCKED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( COMMENT ~ ^Eq ~ ^#literal_string)?
        },
        |(
            _,
            opt_or_replace,
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
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let stmt = CreateNetworkPolicyStmt {
                create_option,
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
            Ok(Statement::CreateNetworkPolicy(stmt))
        },
    );
    let alter_network_policy = map(
        rule! {
            ALTER ~ NETWORK ~ ^POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident ~ SET
             ~ ( ALLOWED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( BLOCKED_IP_LIST ~ ^Eq ~ ^"(" ~ ^#comma_separated_list0(literal_string) ~ ^")" ) ?
             ~ ( COMMENT ~ ^Eq ~ ^#literal_string)?
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
            DROP ~ NETWORK ~ ^POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
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
            ( DESC | DESCRIBE ) ~ NETWORK ~ ^POLICY ~ ^#ident
        },
        |(_, _, _, name)| {
            Statement::DescNetworkPolicy(DescNetworkPolicyStmt {
                name: name.to_string(),
            })
        },
    );
    let show_network_policies = value(
        Statement::ShowNetworkPolicies,
        rule! { SHOW ~ NETWORK ~ ^POLICIES },
    );

    let create_password_policy = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ PASSWORD ~ ^POLICY ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ ^#ident
             ~ #password_set_options
        },
        |(_, opt_or_replace, _, _, opt_if_not_exists, name, set_options)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            let stmt = CreatePasswordPolicyStmt {
                create_option,
                name: name.to_string(),
                set_options,
            };
            Ok(Statement::CreatePasswordPolicy(stmt))
        },
    );
    let alter_password_policy = map(
        rule! {
            ALTER ~ PASSWORD ~ ^POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
             ~ #alter_password_action
        },
        |(_, _, _, opt_if_exists, name, action)| {
            let stmt = AlterPasswordPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                action,
            };
            Statement::AlterPasswordPolicy(stmt)
        },
    );
    let drop_password_policy = map(
        rule! {
            DROP ~ PASSWORD ~ ^POLICY ~ ( IF ~ ^EXISTS )? ~ ^#ident
        },
        |(_, _, _, opt_if_exists, name)| {
            let stmt = DropPasswordPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
            };
            Statement::DropPasswordPolicy(stmt)
        },
    );
    let describe_password_policy = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PASSWORD ~ ^POLICY ~ ^#ident
        },
        |(_, _, _, name)| {
            Statement::DescPasswordPolicy(DescPasswordPolicyStmt {
                name: name.to_string(),
            })
        },
    );
    let show_password_policies = map(
        rule! {
            SHOW ~ PASSWORD ~ ^POLICIES ~ ^#show_options?
        },
        |(_, _, _, show_options)| Statement::ShowPasswordPolicies { show_options },
    );

    let create_pipe = map(
        rule! {
            CREATE ~ PIPE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ( AUTO_INGEST ~ "=" ~ #literal_bool )?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
            ~ AS ~ #copy_into_table
        },
        |(_, _, opt_if_not_exists, pipe, ingest, comment_opt, _, copy_stmt)| {
            let copy_stmt = match copy_stmt {
                Statement::CopyIntoTable(stmt) => stmt,
                _ => {
                    unreachable!()
                }
            };
            Statement::CreatePipe(CreatePipeStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                name: pipe.to_string(),
                auto_ingest: ingest.map(|v| v.2).unwrap_or_default(),
                comments: comment_opt.map(|v| v.2).unwrap_or_default(),
                copy_stmt,
            })
        },
    );

    let alter_pipe = map(
        rule! {
            ALTER ~ PIPE ~ ( IF ~ ^EXISTS )?
            ~ #ident ~ #alter_pipe_option
        },
        |(_, _, opt_if_exists, task, options)| {
            Statement::AlterPipe(AlterPipeStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
                options,
            })
        },
    );

    let drop_pipe = map(
        rule! {
            DROP ~ PIPE ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, opt_if_exists, task)| {
            Statement::DropPipe(DropPipeStmt {
                if_exists: opt_if_exists.is_some(),
                name: task.to_string(),
            })
        },
    );

    let desc_pipe = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PIPE ~ #ident
        },
        |(_, _, task)| {
            Statement::DescribePipe(DescribePipeStmt {
                name: task.to_string(),
            })
        },
    );
    let create_notification = map(
        rule! {
            CREATE ~ NOTIFICATION ~ INTEGRATION
            ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ TYPE ~ "=" ~ #ident
            ~ ENABLED ~ "=" ~ #literal_bool
            ~ #notification_webhook_clause?
            ~ ( (COMMENT | COMMENTS) ~ ^"=" ~ ^#literal_string )?
        },
        |(
            _,
            _,
            _,
            if_not_exists,
            name,
            _,
            _,
            notification_type,
            _,
            _,
            enabled,
            webhook,
            comment,
        )| {
            Statement::CreateNotification(CreateNotificationStmt {
                if_not_exists: if_not_exists.is_some(),
                name: name.to_string(),
                notification_type: notification_type.to_string(),
                enabled,
                webhook_opts: webhook,
                comments: comment.map(|(_, _, comments)| comments),
            })
        },
    );

    let drop_notification = map(
        rule! {
            DROP ~ NOTIFICATION ~ INTEGRATION ~ ( IF ~ ^EXISTS )?
            ~ #ident
        },
        |(_, _, _, if_exists, name)| {
            Statement::DropNotification(DropNotificationStmt {
                if_exists: if_exists.is_some(),
                name: name.to_string(),
            })
        },
    );

    let alter_notification = map(
        rule! {
            ALTER ~ NOTIFICATION ~ INTEGRATION ~ ( IF ~ ^EXISTS )?
            ~ #ident
            ~ #alter_notification_options
        },
        |(_, _, _, if_exists, name, options)| {
            Statement::AlterNotification(AlterNotificationStmt {
                if_exists: if_exists.is_some(),
                name: name.to_string(),
                options,
            })
        },
    );

    let desc_notification = map(
        rule! {
            ( DESC | DESCRIBE ) ~ NOTIFICATION ~ INTEGRATION ~ #ident
        },
        |(_, _, _, name)| {
            Statement::DescribeNotification(DescribeNotificationStmt {
                name: name.to_string(),
            })
        },
    );

    let begin = value(Statement::Begin, rule! { BEGIN ~ TRANSACTION? });
    let commit = value(Statement::Commit, rule! { COMMIT });
    let abort = value(Statement::Abort, rule! { ABORT | ROLLBACK });

    let execute_immediate = map(
        rule! {
            EXECUTE ~ IMMEDIATE ~ #code_string
        },
        |(_, _, script)| Statement::ExecuteImmediate(ExecuteImmediateStmt { script }),
    );

    let system_action = map(
        rule! {
            SYSTEM ~ #action
        },
        |(_, action)| Statement::System(SystemStmt { action }),
    );

    pub fn procedure_type(i: Input) -> IResult<ProcedureType> {
        map(rule! { #ident ~ #type_name }, |(name, data_type)| {
            ProcedureType {
                name: Some(name.to_string()),
                data_type,
            }
        })(i)
    }

    fn procedure_return(i: Input) -> IResult<Vec<ProcedureType>> {
        let procedure_table_return = map(
            rule! {
                TABLE ~ "(" ~ #comma_separated_list1(procedure_type) ~ ")"
            },
            |(_, _, test, _)| test,
        );
        let procedure_single_return = map(rule! { #type_name }, |data_type| {
            vec![ProcedureType {
                name: None,
                data_type,
            }]
        });
        rule!(#procedure_single_return: "<type_name>"
            | #procedure_table_return: "TABLE(<var_name> <type_name>, ...)")(i)
    }

    fn procedure_arg(i: Input) -> IResult<Option<Vec<ProcedureType>>> {
        let procedure_args = map(
            rule! {
                "(" ~ #comma_separated_list1(procedure_type) ~ ")"
            },
            |(_, args, _)| Some(args),
        );
        let procedure_empty_args = map(
            rule! {
                "(" ~ ")"
            },
            |(_, _)| None,
        );
        rule!(#procedure_empty_args: "()"
            | #procedure_args: "(<var_name> <type_name>, ...)")(i)
    }

    // CREATE [ OR REPLACE ] PROCEDURE <name> ()
    // RETURNS { <result_data_type> }[ NOT NULL ]
    // LANGUAGE SQL
    // [ COMMENT = '<string_literal>' ] AS <procedure_definition>
    let create_procedure = map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ PROCEDURE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #ident ~ #procedure_arg ~ RETURNS ~ #procedure_return ~ LANGUAGE ~ SQL  ~ (COMMENT ~ "=" ~ #literal_string)? ~ AS ~ #code_string
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            name,
            args,
            _,
            return_type,
            _,
            _,
            opt_comment,
            _,
            script,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;

            let name = ProcedureIdentity {
                name: name.to_string(),
                args_type: if let Some(args) = &args {
                    args.iter()
                        .map(|arg| arg.data_type.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                } else {
                    "".to_string()
                },
            };
            let stmt = CreateProcedureStmt {
                create_option,
                name,
                args,
                return_type,
                language: ProcedureLanguage::SQL,
                comment: match opt_comment {
                    Some(opt) => Some(opt.2),
                    None => None,
                },
                script,
            };
            Ok(Statement::CreateProcedure(stmt))
        },
    );

    let show_procedures = map(
        rule! {
            SHOW ~ PROCEDURES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowProcedures { show_options },
    );

    fn procedure_type_name(i: Input) -> IResult<Vec<TypeName>> {
        let procedure_type_names = map(
            rule! {
                "(" ~ #comma_separated_list1(type_name) ~ ")"
            },
            |(_, args, _)| args,
        );
        let procedure_empty_types = map(
            rule! {
                "(" ~ ")"
            },
            |(_, _)| vec![],
        );
        rule!(#procedure_empty_types: "()"
            | #procedure_type_names: "(<type_name>, ...)")(i)
    }

    let call_procedure = map(
        rule! {
            CALL ~ PROCEDURE ~ #ident ~ "(" ~ #comma_separated_list0(subexpr(0))? ~ ")"
        },
        |(_, _, name, _, opt_args, _)| {
            Statement::CallProcedure(CallProcedureStmt {
                name: name.to_string(),
                args: opt_args.unwrap_or_default(),
            })
        },
    );

    let drop_procedure = map(
        rule! {
            DROP ~ PROCEDURE ~ ( IF ~ ^EXISTS )? ~ #ident ~ #procedure_type_name
        },
        |(_, _, opt_if_exists, name, args)| {
            Statement::DropProcedure(DropProcedureStmt {
                if_exists: opt_if_exists.is_some(),
                name: ProcedureIdentity {
                    name: name.to_string(),
                    args_type: if args.is_empty() {
                        "".to_string()
                    } else {
                        args.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<String>>()
                            .join(",")
                    },
                },
            })
        },
    );

    let describe_procedure = map(
        rule! {
            ( DESC | DESCRIBE ) ~ PROCEDURE ~ #ident ~ #procedure_type_name
        },
        |(_, _, name, args)| {
            // TODO: modify to ProcedureIdentify
            Statement::DescProcedure(DescProcedureStmt {
                name: ProcedureIdentity {
                    name: name.to_string(),
                    args_type: if args.is_empty() {
                        "".to_string()
                    } else {
                        args.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<String>>()
                            .join(",")
                    },
                },
            })
        },
    );

    alt((
        // query, explain,show
        rule!(
            #map(query, |query| Statement::Query(Box::new(query)))
            | #explain : "`EXPLAIN [PIPELINE | GRAPH] <statement>`"
            | #explain_analyze : "`EXPLAIN ANALYZE <statement>`"
            | #show_settings : "`SHOW SETTINGS [<show_limit>]`"
            | #show_variables : "`SHOW VARIABLES [<show_limit>]`"
            | #show_stages : "`SHOW STAGES`"
            | #show_engines : "`SHOW ENGINES`"
            | #show_process_list : "`SHOW PROCESSLIST`"
            | #show_metrics : "`SHOW METRICS`"
            | #show_functions : "`SHOW FUNCTIONS [<show_limit>]`"
            | #show_indexes : "`SHOW INDEXES`"
            | #show_locks : "`SHOW LOCKS [IN ACCOUNT] [WHERE ...]`"
            | #kill_stmt : "`KILL (QUERY | CONNECTION) <object_id>`"
            | #vacuum_temp_files : "VACUUM TEMPORARY FILES [RETAIN number SECONDS|DAYS] [LIMIT number]"
            | #set_priority: "`SET PRIORITY (HIGH | MEDIUM | LOW) <object_id>`"
            | #system_action: "`SYSTEM (ENABLE | DISABLE) EXCEPTION_BACKTRACE`"
        ),
        // use
        rule!(
                #use_catalog: "`USE CATALOG <catalog>`"
                | #use_warehouse: "`USE WAREHOUSE <warehouse>`"
                | #use_database : "`USE <database>`"
        ),
        // warehouse
        rule!(
            #show_warehouses: "`SHOW WAREHOUSES`"
            | #show_online_nodes: "`SHOW ONLINE NODES`"
            | #create_warehouse: "`CREATE WAREHOUSE <warehouse> [(ASSIGN <node_size> NODES [FROM <node_group>] [, ...])] WITH [warehouse_size = <warehouse_size>]`"
            | #drop_warehouse: "`DROP WAREHOUSE <warehouse>`"
            | #rename_warehouse: "`RENAME WAREHOUSE <warehouse> TO <new_warehouse>`"
            | #resume_warehouse: "`RESUME WAREHOUSE <warehouse>`"
            | #suspend_warehouse: "`SUSPEND WAREHOUSE <warehouse>`"
            | #inspect_warehouse: "`INSPECT WAREHOUSE <warehouse>`"
            | #add_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> ADD CLUSTER <cluster> [(ASSIGN <node_size> NODES [FROM <node_group>] [, ...])] WITH [cluster_size = <cluster_size>]`"
            | #drop_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> DROP CLUSTER <cluster>`"
            | #rename_warehouse_cluster: "`ALTER WAREHOUSE <warehouse> RENAME CLUSTER <cluster> TO <new_cluster>`"
            | #assign_warehouse_nodes: "`ALTER WAREHOUSE <warehouse> ASSIGN NODES ( ASSIGN <node_size> NODES [FROM <node_group>] FOR <cluster> [, ...] )`"
            | #unassign_warehouse_nodes: "`ALTER WAREHOUSE <warehouse> UNASSIGN NODES ( UNASSIGN <node_size> NODES [FROM <node_group>] FOR <cluster> [, ...] )`"
        ),
        // database
        rule!(
            #show_databases : "`SHOW [FULL] DATABASES [(FROM | IN) <catalog>] [<show_limit>]`"
            | #undrop_database : "`UNDROP DATABASE <database>`"
            | #show_create_database : "`SHOW CREATE DATABASE <database>`"
            | #show_drop_databases : "`SHOW DROP DATABASES [FROM <database>] [<show_limit>]`"
            | #create_database : "`CREATE [OR REPLACE] DATABASE [IF NOT EXISTS] <database> [ENGINE = <engine>]`"
            | #drop_database : "`DROP DATABASE [IF EXISTS] <database>`"
            | #alter_database : "`ALTER DATABASE [IF EXISTS] <action>`"
        ),
        // network policy / password policy
        rule!(
            #create_network_policy: "`CREATE NETWORK POLICY [IF NOT EXISTS] name ALLOWED_IP_LIST = ('ip1' [, 'ip2']) [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
            | #alter_network_policy: "`ALTER NETWORK POLICY [IF EXISTS] name SET [ALLOWED_IP_LIST = ('ip1' [, 'ip2'])] [BLOCKED_IP_LIST = ('ip1' [, 'ip2'])] [COMMENT = '<string_literal>']`"
            | #drop_network_policy: "`DROP NETWORK POLICY [IF EXISTS] name`"
            | #describe_network_policy: "`DESC NETWORK POLICY name`"
            | #show_network_policies: "`SHOW NETWORK POLICIES`"
            | #create_password_policy: "`CREATE PASSWORD POLICY [IF NOT EXISTS] name [PASSWORD_MIN_LENGTH = <u64_literal>] ... [COMMENT = '<string_literal>']`"
            | #alter_password_policy: "`ALTER PASSWORD POLICY [IF EXISTS] name SET [PASSWORD_MIN_LENGTH = <u64_literal>] ... [COMMENT = '<string_literal>']`"
            | #drop_password_policy: "`DROP PASSWORD POLICY [IF EXISTS] name`"
            | #describe_password_policy: "`DESC PASSWORD POLICY name`"
            | #show_password_policies: "`SHOW PASSWORD POLICIES [<show_options>]`"
        ),
        rule!(
            #conditional_multi_table_insert() : "`INSERT [OVERWRITE] {FIRST|ALL} { WHEN <condition> THEN intoClause [ ... ] } [ ... ] [ ELSE intoClause ] <subquery>`"
            | #unconditional_multi_table_insert() : "`INSERT [OVERWRITE] ALL intoClause [ ... ] <subquery>`"
            | #insert_stmt(false) : "`INSERT INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #replace_stmt(false) : "`REPLACE INTO [TABLE] <table> [(<column>, ...)] (FORMAT <format> | VALUES <values> | <query>)`"
            | #merge : "`MERGE INTO <target_table> USING <source> ON <join_expr> { matchedClause | notMatchedClause } [ ... ]`"
            | #delete : "`DELETE FROM <table> [WHERE ...]`"
            | #update : "`UPDATE <table> SET <column> = <expr> [, <column> = <expr> , ... ] [WHERE ...]`"
            | #begin
            | #commit
            | #abort
        ),
        rule!(
            #show_users : "`SHOW USERS`"
            | #describe_user: "`DESCRIBE USER <user_name>`"
            | #create_user : "`CREATE [OR REPLACE] USER [IF NOT EXISTS] '<username>' IDENTIFIED [WITH <auth_type>] [BY <password>] [WITH <user_option>, ...]`"
            | #alter_user : "`ALTER USER ('<username>' | USER()) [IDENTIFIED [WITH <auth_type>] [BY <password>]] [WITH <user_option>, ...]`"
            | #drop_user : "`DROP USER [IF EXISTS] '<username>'`"
            | #show_roles : "`SHOW ROLES`"
            | #create_role : "`CREATE ROLE [IF NOT EXISTS] <role_name>`"
            | #drop_role : "`DROP ROLE [IF EXISTS] <role_name>`"
            | #create_udf : "`CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] <udf_name> <udf_definition> [DESC = <description>]`"
            | #drop_udf : "`DROP FUNCTION [IF EXISTS] <udf_name>`"
            | #alter_udf : "`ALTER FUNCTION <udf_name> <udf_definition> [DESC = <description>]`"
            | #set_role: "`SET [DEFAULT] ROLE <role>`"
            | #set_secondary_roles: "`SET SECONDARY ROLES (ALL | NONE)`"
            | #show_user_functions : "`SHOW USER FUNCTIONS [<show_limit>]`"
        ),
        rule!(
            #show_tables : "`SHOW [FULL] TABLES [FROM <database>] [<show_limit>]`"
            | #show_columns : "`SHOW [FULL] COLUMNS FROM <table> [FROM|IN <catalog>.<database>] [<show_limit>]`"
            | #show_create_table : "`SHOW CREATE TABLE [<database>.]<table>`"
            | #describe_view : "`DESCRIBE VIEW [<database>.]<view>`"
            | #describe_table : "`DESCRIBE [<database>.]<table>`"
            | #show_fields : "`SHOW FIELDS FROM [<database>.]<table>`"
            | #show_tables_status : "`SHOW TABLES STATUS [FROM <database>] [<show_limit>]`"
            | #show_drop_tables_status : "`SHOW DROP TABLES [FROM <database>]`"
            | #attach_table : "`ATTACH TABLE [<database>.]<table> <uri>`"
            | #create_table : "`CREATE [OR REPLACE] TABLE [IF NOT EXISTS] [<database>.]<table> [<source>] [<table_options>]`"
            | #drop_table : "`DROP TABLE [IF EXISTS] [<database>.]<table>`"
            | #undrop_table : "`UNDROP TABLE [<database>.]<table>`"
            | #alter_table : "`ALTER TABLE [<database>.]<table> <action>`"
            | #rename_table : "`RENAME TABLE [<database>.]<table> TO <new_table>`"
            | #truncate_table : "`TRUNCATE TABLE [<database>.]<table>`"
            | #optimize_table : "`OPTIMIZE TABLE [<database>.]<table> (ALL | PURGE | COMPACT [SEGMENT])`"
            | #vacuum_table : "`VACUUM TABLE [<database>.]<table> [RETAIN number HOURS] [DRY RUN | DRY RUN SUMMARY]`"
            | #vacuum_drop_table : "`VACUUM DROP TABLE [FROM [<catalog>.]<database>] [RETAIN number HOURS] [DRY RUN | DRY RUN SUMMARY]`"
            | #analyze_table : "`ANALYZE TABLE [<database>.]<table>`"
            | #exists_table : "`EXISTS TABLE [<database>.]<table>`"
            | #show_table_functions : "`SHOW TABLE_FUNCTIONS [<show_limit>]`"
        ),
        // dictionary
        rule!(
            #create_dictionary : "`CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] <dictionary_name> [(<column>, ...)] PRIMARY KEY [<primary_key>, ...] SOURCE (<source_name> ([<source_options>])) [COMMENT <comment>] `"
            | #drop_dictionary : "`DROP DICTIONARY [IF EXISTS] <dictionary_name>`"
            | #show_create_dictionary : "`SHOW CREATE DICTIONARY <dictionary_name> `"
            | #show_dictionaries : "`SHOW DICTIONARIES [<show_option>, ...]`"
            | #rename_dictionary: "`RENAME DICTIONARY [<database>.]<old_dict_name> TO <new_dict_name>`"
        ),
        // view,index
        rule!(
            #create_view : "`CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
            | #drop_view : "`DROP VIEW [IF EXISTS] [<database>.]<view>`"
            | #alter_view : "`ALTER VIEW [<database>.]<view> [(<column>, ...)] AS SELECT ...`"
            | #show_views : "`SHOW [FULL] VIEWS [FROM <database>] [<show_limit>]`"
            | #create_index: "`CREATE [OR REPLACE] AGGREGATING INDEX [IF NOT EXISTS] <index> AS SELECT ...`"
            | #drop_index: "`DROP <index_type> INDEX [IF EXISTS] <index>`"
            | #refresh_index: "`REFRESH <index_type> INDEX <index> [LIMIT <limit>]`"
            | #create_inverted_index: "`CREATE [OR REPLACE] INVERTED INDEX [IF NOT EXISTS] <index> ON [<database>.]<table>(<column>, ...)`"
            | #drop_inverted_index: "`DROP INVERTED INDEX [IF EXISTS] <index> ON [<database>.]<table>`"
            | #refresh_inverted_index: "`REFRESH INVERTED INDEX <index> ON [<database>.]<table> [LIMIT <limit>]`"
        ),
        rule!(
            #create_virtual_column: "`CREATE VIRTUAL COLUMN (expr, ...) FOR [<database>.]<table>`"
            | #alter_virtual_column: "`ALTER VIRTUAL COLUMN (expr, ...) FOR [<database>.]<table>`"
            | #drop_virtual_column: "`DROP VIRTUAL COLUMN FOR [<database>.]<table>`"
            | #refresh_virtual_column: "`REFRESH VIRTUAL COLUMN FOR [<database>.]<table>`"
            | #show_virtual_columns : "`SHOW VIRTUAL COLUMNS FROM <table> [FROM|IN <catalog>.<database>] [<show_limit>]`"
            | #sequence
        ),
        rule!(
            #create_stage: "`CREATE [OR REPLACE] STAGE [ IF NOT EXISTS ] <stage_name>
                [ FILE_FORMAT = ( { TYPE = { CSV | PARQUET } [ formatTypeOptions ] ) } ]
                [ COPY_OPTIONS = ( copyOptions ) ]
                [ COMMENT = '<string_literal>' ]`"
            | #desc_stage: "`DESC STAGE <stage_name>`"
            | #list_stage: "`LIST @<stage_name> [pattern = '<pattern>']`"
            | #remove_stage: "`REMOVE @<stage_name> [pattern = '<pattern>']`"
            | #drop_stage: "`DROP STAGE <stage_name>`"
            | #create_file_format: "`CREATE FILE FORMAT [ IF NOT EXISTS ] <format_name> formatTypeOptions`"
            | #show_file_formats: "`SHOW FILE FORMATS`"
            | #drop_file_format: "`DROP FILE FORMAT  [ IF EXISTS ] <format_name>`"
            | #copy_into
            | #call: "`CALL <procedure_name>(<parameter>, ...)`"
            | #grant : "`GRANT { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } TO { [ROLE <role_name>] | [USER] <user> }`"
            | #show_grants : "`SHOW GRANTS {FOR  { ROLE <role_name> | USER <user> }] | ON {DATABASE <db_name> | TABLE <db_name>.<table_name>} }`"
            | #revoke : "`REVOKE { ROLE <role_name> | schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level> } FROM { [ROLE <role_name>] | [USER] <user> }`"
            | #grant_ownership : "GRANT OWNERSHIP ON <privileges_level> TO ROLE <role_name>"
            | #presign: "`PRESIGN [{DOWNLOAD | UPLOAD}] <location> [EXPIRE = 3600]`"
        ),
        // data mask
        rule!(
            #create_data_mask_policy: "`CREATE MASKING POLICY [IF NOT EXISTS] mask_name as (val1 val_type1 [, val type]) return type -> case`"
            | #drop_data_mask_policy: "`DROP MASKING POLICY [IF EXISTS] mask_name`"
            | #describe_data_mask_policy: "`DESC MASKING POLICY mask_name`"
        ),
        rule!(
            #set_stmt : "`SET [variable] {<name> = <value> | (<name>, ...) = (<value>, ...)}`"
            | #unset_stmt : "`UNSET [variable] {<name> | (<name>, ...)}`"
            | #query_setting : "SETTINGS ( {<name> = <value> | (<name>, ...) = (<value>, ...)} )  Statement"
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
  [ { WAREHOUSE = <string> } ]
  [ SCHEDULE = { <num> MINUTE | USING CRON <expr> <time_zone> } ]
  [ AFTER <string>, <string>...]
  [ WHEN boolean_expr ]
  [ SUSPEND_TASK_AFTER_NUM_FAILURES = <num> ]
  [ ERROR_INTEGRATION = <string_literal> ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
         | #drop_task : "`DROP TASK [ IF EXISTS ] <name>`"
         | #alter_task : "`ALTER TASK [ IF EXISTS ] <name> SUSPEND | RESUME | SET <option> = <value>` | UNSET <option> | MODIFY AS <sql> | MODIFY WHEN <boolean_expr> | ADD/REMOVE AFTER <string>, <string>...`"
         | #show_tasks : "`SHOW TASKS [<show_limit>]`"
         | #desc_task : "`DESC | DESCRIBE TASK <name>`"
         | #execute_task: "`EXECUTE TASK <name>`"
        ),
        // stream, dynamic tables.
        rule!(
            #stream_table
            | #dynamic_table
        ),
        rule!(
            #create_pipe : "`CREATE PIPE [ IF NOT EXISTS ] <name>
  [ AUTO_INGEST = [ TRUE | FALSE ] ]
  [ COMMENT = '<string_literal>' ]
AS
  <copy_sql>`"
            | #drop_pipe : "`DROP PIPE [ IF EXISTS ] <name>`"
            | #alter_pipe : "`ALTER PIPE [ IF EXISTS ] <name> SET <option> = <value>` | REFRESH <option> = <value>`"
            | #desc_pipe : "`DESC | DESCRIBE PIPE <name>`"
            | #create_notification : "`CREATE NOTIFICATION INTEGRATION [ IF NOT EXISTS ] <name>
    TYPE = <type>
    ENABLED = <bool>
    [ WEBHOOK = ( url = <string_literal>, method = <string_literal>, authorization_header = <string_literal> ) ]
    [ COMMENT = '<string_literal>' ]`"
            | #alter_notification : "`ALTER NOTIFICATION INTEGRATION [ IF EXISTS ] <name> SET <option> = <value>`"
            | #desc_notification : "`DESC | DESCRIBE NOTIFICATION INTEGRATION <name>`"
            | #drop_notification : "`DROP NOTIFICATION INTEGRATION [ IF EXISTS ] <name>`"
        ),
        rule!(
            #create_connection: "`CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] <connection_name> STORAGE_TYPE = <type> <storage_configs>`"
            | #drop_connection: "`DROP CONNECTION [IF EXISTS] <connection_name>`"
            | #desc_connection: "`DESC | DESCRIBE CONNECTION  <connection_name>`"
            | #show_connections: "`SHOW CONNECTIONS`"
            | #execute_immediate : "`EXECUTE IMMEDIATE $$ <script> $$`"
            | #create_procedure : "`CREATE [ OR REPLACE ] PROCEDURE <procedure_name>() RETURNS { <result_data_type> [ NOT NULL ] | TABLE(<var_name> <data_type>, ...)} LANGUAGE SQL [ COMMENT = '<string_literal>' ] AS <procedure_definition>`"
            | #drop_procedure : "`DROP PROCEDURE <procedure_name>()`"
            | #show_procedures : "`SHOW PROCEDURES [<show_options>]()`"
            | #describe_procedure : "`DESC PROCEDURE <procedure_name>()`"
            | #call_procedure : "`CALL PROCEDURE <procedure_name>()`"
        ),
    ))(i)
}

pub fn statement(i: Input) -> IResult<StatementWithFormat> {
    map(
        rule! {
            #statement_body ~ ( FORMAT ~ ^#ident )? ~ ";"? ~ &EOI
        },
        |(stmt, opt_format, _, _)| StatementWithFormat {
            stmt,
            format: opt_format.map(|(_, format)| format.name),
        },
    )(i)
}

pub fn parse_create_option(
    opt_or_replace: bool,
    opt_if_not_exists: bool,
) -> Result<CreateOption, nom::Err<ErrorKind>> {
    match (opt_or_replace, opt_if_not_exists) {
        (false, false) => Ok(CreateOption::Create),
        (true, false) => Ok(CreateOption::CreateOrReplace),
        (false, true) => Ok(CreateOption::CreateIfNotExists),
        (true, true) => Err(nom::Err::Failure(ErrorKind::Other(
            "option IF NOT EXISTS and OR REPLACE are incompatible.",
        ))),
    }
}

pub fn insert_stmt(allow_raw: bool) -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        let insert_source_parser = if allow_raw {
            raw_insert_source
        } else {
            insert_source
        };
        map(
            rule! {
                #with? ~ INSERT ~ #hint? ~ ( INTO | OVERWRITE ) ~ TABLE?
                ~ #dot_separated_idents_1_to_3
                ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
                ~ #insert_source_parser
            },
            |(
                with,
                _,
                opt_hints,
                overwrite,
                _,
                (catalog, database, table),
                opt_columns,
                source,
            )| {
                Statement::Insert(InsertStmt {
                    hints: opt_hints,
                    with,
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
        )(i)
    }
}

pub fn conditional_multi_table_insert() -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        map(
            rule! {
                INSERT ~ OVERWRITE? ~ (FIRST | ALL) ~ (#when_clause)+ ~ (#else_clause)? ~ #query
            },
            |(_, overwrite, kind, when_clauses, opt_else, source)| {
                Statement::InsertMultiTable(InsertMultiTableStmt {
                    overwrite: overwrite.is_some(),
                    is_first: matches!(kind.kind, FIRST),
                    when_clauses,
                    else_clause: opt_else,
                    into_clauses: vec![],
                    source,
                })
            },
        )(i)
    }
}

pub fn unconditional_multi_table_insert() -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        map(
            rule! {
                INSERT ~ OVERWRITE? ~ ALL ~ (#into_clause)+ ~ #query
            },
            |(_, overwrite, _, into_clauses, source)| {
                Statement::InsertMultiTable(InsertMultiTableStmt {
                    overwrite: overwrite.is_some(),
                    is_first: false,
                    when_clauses: vec![],
                    else_clause: None,
                    into_clauses,
                    source,
                })
            },
        )(i)
    }
}

fn when_clause(i: Input) -> IResult<WhenClause> {
    map(
        rule! {
            WHEN ~ ^#expr ~ THEN ~ (#into_clause)+
        },
        |(_, expr, _, into_clauses)| WhenClause {
            condition: expr,
            into_clauses,
        },
    )(i)
}

fn into_clause(i: Input) -> IResult<IntoClause> {
    let source_expr = alt((
        map(rule! {DEFAULT}, |_| SourceExpr::Default),
        map(rule! { #expr }, SourceExpr::Expr),
    ));
    map(
        rule! {
            INTO
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ (VALUES ~ "(" ~ #comma_separated_list1(source_expr) ~ ")" )?
        },
        |(_, (catalog, database, table), opt_target_columns, opt_source_columns)| IntoClause {
            catalog,
            database,
            table,
            target_columns: opt_target_columns
                .map(|(_, columns, _)| columns)
                .unwrap_or_default(),
            source_columns: opt_source_columns
                .map(|(_, _, columns, _)| columns)
                .unwrap_or_default(),
        },
    )(i)
}

fn else_clause(i: Input) -> IResult<ElseClause> {
    map(
        rule! {
            ELSE ~ (#into_clause)+
        },
        |(_, into_clauses)| ElseClause { into_clauses },
    )(i)
}

pub fn replace_stmt(allow_raw: bool) -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        let insert_source_parser = if allow_raw {
            raw_insert_source
        } else {
            insert_source
        };
        map(
            rule! {
                REPLACE ~ #hint? ~ INTO?
                ~ #dot_separated_idents_1_to_3
                ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
                ~ ON ~ CONFLICT? ~ "(" ~ #comma_separated_list1(ident) ~ ")"
                ~ ( DELETE ~ WHEN ~ ^#expr )?
                ~ #insert_source_parser
            },
            |(
                _,
                opt_hints,
                _,
                (catalog, database, table),
                opt_columns,
                _,
                _,
                _,
                on_conflict_columns,
                _,
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
        )(i)
    }
}

// `VALUES (expr, expr), (expr, expr)`
pub fn insert_source(i: Input) -> IResult<InsertSource> {
    let row = map(
        rule! {
            "(" ~ #comma_separated_list1(expr) ~ ")"
        },
        |(_, values, _)| values,
    );
    let values = map(
        rule! {
            VALUES ~ #comma_separated_list0(row)
        },
        |(_, rows)| InsertSource::Values { rows },
    );

    let query = map(query, |query| InsertSource::Select {
        query: Box::new(query),
    });

    rule!(
        #values
        | #query
    )(i)
}

// `INSERT INTO ... VALUES` statement will
// stop the parser immediately and return the rest tokens in `InsertSource`.
//
// This is a hack to parse large insert statements.
pub fn raw_insert_source(i: Input) -> IResult<InsertSource> {
    let values = map(
        rule! {
            VALUES ~ #rest_str
        },
        |(_, (rest_str, start))| InsertSource::RawValues { rest_str, start },
    );
    let query = map(
        rule! {
            #query ~ ";"? ~ &EOI
        },
        |(query, _, _)| InsertSource::Select {
            query: Box::new(query),
        },
    );

    rule!(
        #values
        | #query
    )(i)
}

pub fn mutation_source(i: Input) -> IResult<MutationSource> {
    let streaming_v2 = map(
        rule! {
           #file_format_clause  ~ (ON_ERROR ~ ^"=" ~ ^#ident)? ~  #rest_str
        },
        |(options, on_error_opt, (_, start))| MutationSource::StreamingV2 {
            settings: options,
            on_error_mode: on_error_opt.map(|v| v.2.to_string()),
            start,
        },
    );

    let query = map(rule! {#query ~ #table_alias}, |(query, source_alias)| {
        MutationSource::Select {
            query: Box::new(query),
            source_alias,
        }
    });

    let source_table = map(
        rule!(#dot_separated_idents_1_to_3 ~ #with_options? ~ #table_alias?),
        |((catalog, database, table), with_options, alias)| MutationSource::Table {
            catalog,
            database,
            table,
            with_options,
            alias,
        },
    );

    rule!(
          #streaming_v2
        | #query
        | #source_table
    )(i)
}

pub fn unset_source(i: Input) -> IResult<Vec<Identifier>> {
    //#ident ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ")")?
    let var = map(
        rule! {
            #ident
        },
        |variable| vec![variable],
    );
    let vars = map(
        rule! {
            "(" ~ ^#comma_separated_list1(ident) ~ ")"
        },
        |(_, variables, _)| variables,
    );

    rule!(
        #var
        | #vars
    )(i)
}

pub fn set_stmt_args(i: Input) -> IResult<(Identifier, Box<Expr>)> {
    map(
        rule! {
            #ident ~ "=" ~ #subexpr(0)
        },
        |(id, _, expr)| (id, Box::new(expr)),
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

pub fn query_setting(i: Input) -> IResult<(Identifier, Expr)> {
    map(
        rule! {
            #ident ~ "=" ~ #subexpr(0)
        },
        |(id, _, value)| (id, value),
    )(i)
}

pub fn query_statement_setting(i: Input) -> IResult<Settings> {
    let query_set = map(
        rule! {
            "(" ~ #comma_separated_list0(query_setting) ~ ")"
        },
        |(_, query_setting, _)| {
            let mut ids = Vec::with_capacity(query_setting.len());
            let mut values = Vec::with_capacity(query_setting.len());
            for (id, value) in query_setting {
                ids.push(id);
                values.push(value);
            }
            Settings {
                set_type: SetType::SettingsQuery,
                identifiers: ids,
                values: SetValues::Expr(values.into_iter().map(|x| x.into()).collect()),
            }
        },
    );
    rule!(#query_set: "(SETTING_NAME = VALUE, ...)")(i)
}
pub fn top_n(i: Input) -> IResult<u64> {
    map(
        rule! {
            TOP
            ~ ^#error_hint(
                not(literal_u64),
                "expecting a literal number after keyword `TOP`, if you were referring to a column with name `top`, \
                        please quote it like `\"top\"`"
            )
            ~ ^#literal_u64
            : "TOP <limit>"
        },
        |(_, _, n)| n,
    )(i)
}

pub fn rest_str(i: Input) -> IResult<(String, usize)> {
    // It's safe to unwrap because input must contain EOI.
    let first_token = i.tokens.first().unwrap();
    let last_token = i.tokens.last().unwrap();
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
            };
            (def, constraints)
        },
    )(i)?;

    for constraint in constraints {
        match constraint {
            ColumnConstraint::Nullable(nullable) => {
                if (nullable && matches!(def.data_type, TypeName::NotNull(_)))
                    || (!nullable && matches!(def.data_type, TypeName::Nullable(_)))
                {
                    return Err(nom::Err::Error(Error::from_error_kind(
                        i,
                        ErrorKind::Other("ambiguous NOT NULL constraint"),
                    )));
                }
                if nullable {
                    def.data_type = def.data_type.wrap_nullable();
                } else {
                    def.data_type = def.data_type.wrap_not_null();
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

pub fn inverted_index_def(i: Input) -> IResult<InvertedIndexDefinition> {
    map_res(
        rule! {
            ASYNC?
            ~ INVERTED ~ ^INDEX
            ~ #ident
            ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")"
            ~ ( #table_option )?
        },
        |(opt_async, _, _, index_name, _, columns, _, opt_index_options)| {
            Ok(InvertedIndexDefinition {
                index_name,
                columns,
                sync_creation: opt_async.is_none(),
                index_options: opt_index_options.unwrap_or_default(),
            })
        },
    )(i)
}

pub fn create_def(i: Input) -> IResult<CreateDefinition> {
    alt((
        map(rule! { #column_def }, CreateDefinition::Column),
        map(
            rule! { #inverted_index_def },
            CreateDefinition::InvertedIndex,
        ),
    ))(i)
}

pub fn role_name(i: Input) -> IResult<String> {
    let role_ident = map_res(
        rule! {
            #ident
        },
        |role_name| {
            let name = role_name.name;
            let mut chars = name.chars();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => match chars.next() {
                        Some('f') | Some('b') => {
                            return Err(nom::Err::Failure(ErrorKind::Other(
                                "' or \" or \\f or \\b are not allowed in role name",
                            )));
                        }
                        _ => {}
                    },
                    '\'' | '"' => {
                        return Err(nom::Err::Failure(ErrorKind::Other(
                            "' or \" or \\f or \\b are not allowed in role name",
                        )));
                    }
                    _ => {}
                }
            }
            Ok(name)
        },
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
        rule! { ALL ~ PRIVILEGES? ~ ON ~ #grant_all_level },
        |(_, _, _, level)| AccountMgrSource::ALL { level },
    );

    let udf_privs = map(
        rule! {
            USAGE ~ ON ~ UDF ~ #ident
        },
        |(_, _, _, udf)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::UDF(udf.to_string()),
        },
    );

    let udf_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ UDF ~ #ident
        },
        |(_, _, _, _, udf)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::UDF(udf.to_string()),
        },
    );

    let stage_privs = map(
        rule! {
            #comma_separated_list1(stage_priv_type) ~ ON ~ STAGE ~ #ident
        },
        |(privileges, _, _, stage_name)| AccountMgrSource::Privs {
            privileges,
            level: AccountMgrLevel::Stage(stage_name.to_string()),
        },
    );

    let warehouse_privs = map(
        rule! {
            USAGE ~ ON ~ WAREHOUSE ~ #ident
        },
        |(_, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::Warehouse(w.to_string()),
        },
    );

    let warehouse_all_privs = map(
        rule! {
            ALL ~ PRIVILEGES? ~ ON ~ WAREHOUSE ~ #ident
        },
        |(_, _, _, _, w)| AccountMgrSource::Privs {
            privileges: vec![UserPrivilegeType::Usage],
            level: AccountMgrLevel::Warehouse(w.to_string()),
        },
    );

    rule!(
        #role : "ROLE <role_name>"
        | #warehouse_all_privs: "ALL [ PRIVILEGES ] ON WAREHOUSE <warehouse_name>"
        | #udf_privs: "USAGE ON UDF <udf_name>"
        | #warehouse_privs: "USAGE ON WAREHOUSE <warehouse_name>"
        | #privs : "<privileges> ON <privileges_level>"
        | #stage_privs : "<stage_privileges> ON STAGE <stage_name>"
        | #udf_all_privs: "ALL [ PRIVILEGES ] ON UDF <udf_name>"
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
        value(
            UserPrivilegeType::CreateDatabase,
            rule! { CREATE ~ DATABASE },
        ),
        value(
            UserPrivilegeType::CreateWarehouse,
            rule! { CREATE ~ WAREHOUSE },
        ),
        value(UserPrivilegeType::DropUser, rule! { DROP ~ USER }),
        value(UserPrivilegeType::CreateRole, rule! { CREATE ~ ROLE }),
        value(UserPrivilegeType::DropRole, rule! { DROP ~ ROLE }),
        value(UserPrivilegeType::Grant, rule! { GRANT }),
        value(UserPrivilegeType::CreateStage, rule! { CREATE ~ STAGE }),
        value(UserPrivilegeType::Set, rule! { SET }),
        value(UserPrivilegeType::Drop, rule! { DROP }),
        value(UserPrivilegeType::Create, rule! { CREATE }),
    ))(i)
}

pub fn stage_priv_type(i: Input) -> IResult<UserPrivilegeType> {
    alt((
        value(UserPrivilegeType::Read, rule! { READ }),
        value(UserPrivilegeType::Write, rule! { WRITE }),
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

pub fn on_object_name(i: Input) -> IResult<GrantObjectName> {
    let database = map(
        rule! {
            DATABASE ~ #ident
        },
        |(_, database)| GrantObjectName::Database(database.to_string()),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            TABLE ~  #dot_separated_idents_1_to_2
        },
        |(_, (database, table))| {
            GrantObjectName::Table(database.map(|db| db.to_string()), table.to_string())
        },
    );

    let stage = map(rule! { STAGE ~ #ident}, |(_, stage_name)| {
        GrantObjectName::Stage(stage_name.to_string())
    });

    let udf = map(rule! { UDF ~ #ident}, |(_, udf_name)| {
        GrantObjectName::UDF(udf_name.to_string())
    });

    let warehouse = map(rule! { WAREHOUSE ~ #ident}, |(_, w)| {
        GrantObjectName::Warehouse(w.to_string())
    });

    rule!(
        #database : "DATABASE <database>"
        | #table : "TABLE <database>.<table>"
        | #stage : "STAGE <stage_name>"
        | #udf : "UDF <udf_name>"
        | #warehouse : "WAREHOUSE <warehouse_name>"
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

pub fn grant_all_level(i: Input) -> IResult<AccountMgrLevel> {
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

    let stage = map(rule! { STAGE ~ #ident}, |(_, stage_name)| {
        AccountMgrLevel::Stage(stage_name.to_string())
    });

    let warehouse = map(rule! { WAREHOUSE ~ #ident}, |(_, w)| {
        AccountMgrLevel::Warehouse(w.to_string())
    });
    rule!(
        #global : "*.*"
        | #db : "<database>.*"
        | #table : "<database>.<table>"
        | #stage : "STAGE <stage_name>"
        | #warehouse : "WAREHOUSE <warehouse_name>"
    )(i)
}

pub fn grant_ownership_level(i: Input) -> IResult<AccountMgrLevel> {
    // db.*
    // "*": as current db or "table" with current db
    let db = map(
        rule! {
            ( #grant_ident ~ "." )? ~ "*"
        },
        |(database, _)| AccountMgrLevel::Database(database.map(|(database, _)| database.name)),
    );

    // `db01`.'tb1' or `db01`.`tb1` or `db01`.tb1
    let table = map(
        rule! {
            ( #grant_ident ~ "." )? ~ #parameter_to_grant_string
        },
        |(database, table)| {
            AccountMgrLevel::Table(database.map(|(database, _)| database.name), table)
        },
    );

    #[derive(Clone)]
    enum Object {
        Stage,
        Udf,
        Warehouse,
    }
    let object = alt((
        value(Object::Udf, rule! { UDF }),
        value(Object::Stage, rule! { STAGE }),
        value(Object::Warehouse, rule! { WAREHOUSE }),
    ));

    // Object object_name
    let object = map(
        rule! { #object ~ #grant_ident },
        |(object, object_name)| match object {
            Object::Stage => AccountMgrLevel::Stage(object_name.to_string()),
            Object::Udf => AccountMgrLevel::UDF(object_name.to_string()),
            Object::Warehouse => AccountMgrLevel::Warehouse(object_name.to_string()),
        },
    );

    rule!(
        #db : "<database>.*"
        | #table : "<database>.<table>"
        | #object : "STAGE | UDF | WAREHOUSE <object_name>"
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
            ON ~ #on_object_name
        },
        |(_, object_name)| ShowGrantOption::GrantObjectName(object_name),
    );

    rule!(
        #grant_role: "FOR  { ROLE <role_name> | [USER] <user> }"
        | #share_object_name: "ON {DATABASE <db_name> | TABLE <db_name>.<table_name> | UDF <udf_name> | STAGE <stage_name> }"
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
            "(" ~ ^#comma_separated_list1(create_def) ~ ^")"
        },
        |(_, create_defs, _)| {
            let mut columns = Vec::with_capacity(create_defs.len());
            let mut inverted_indexes = Vec::new();
            for create_def in create_defs {
                match create_def {
                    CreateDefinition::Column(column) => {
                        columns.push(column);
                    }
                    CreateDefinition::InvertedIndex(inverted_index) => {
                        inverted_indexes.push(inverted_index);
                    }
                }
            }
            let opt_inverted_indexes = if !inverted_indexes.is_empty() {
                Some(inverted_indexes)
            } else {
                None
            };
            CreateTableSource::Columns(columns, opt_inverted_indexes)
        },
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
    #[derive(Educe)]
    #[educe(Clone(bound = false, attrs = "#[recursive::recursive]"))]
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

    map_res(
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
            };
            for constraint in constraints {
                match constraint {
                    ColumnConstraint::Nullable(nullable) => {
                        if (nullable && matches!(def.data_type, TypeName::NotNull(_)))
                            || (!nullable && matches!(def.data_type, TypeName::Nullable(_)))
                        {
                            return Err(nom::Err::Failure(ErrorKind::Other(
                                "ambiguous NOT NULL constraint",
                            )));
                        }
                        if nullable {
                            def.data_type = def.data_type.wrap_nullable();
                        } else {
                            def.data_type = def.data_type.wrap_not_null();
                        }
                    }
                    ColumnConstraint::DefaultExpr(default_expr) => {
                        def.expr = Some(ColumnExpr::Default(default_expr))
                    }
                }
            }
            Ok(def)
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
            #modify_column_type ~ ("," ~ COLUMN? ~ #modify_column_type)*
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
            RENAME ~ COLUMN? ~ #ident ~ TO ~ #ident
        },
        |(_, _, old_column, _, new_column)| AlterTableAction::RenameColumn {
            old_column,
            new_column,
        },
    );
    let modify_table_comment = map(
        rule! {
            COMMENT ~ ^"=" ~ ^#literal_string
        },
        |(_, _, new_comment)| AlterTableAction::ModifyTableComment { new_comment },
    );
    let add_column = map(
        rule! {
            ADD ~ COLUMN? ~ #column_def ~ ( #add_column_option )?
        },
        |(_, _, column, option)| AlterTableAction::AddColumn {
            column,
            option: option.unwrap_or(AddColumnOption::End),
        },
    );

    let modify_column = map(
        rule! {
            MODIFY ~ COLUMN? ~ #modify_column_action
        },
        |(_, _, action)| AlterTableAction::ModifyColumn { action },
    );

    let drop_column = map(
        rule! {
            DROP ~ COLUMN? ~ #ident
        },
        |(_, _, column)| AlterTableAction::DropColumn { column },
    );
    let alter_table_cluster_key = map(
        rule! {
            CLUSTER ~ ^BY ~ ( #cluster_type )? ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")"
        },
        |(_, _, typ, _, cluster_exprs, _)| AlterTableAction::AlterTableClusterKey {
            cluster_by: ClusterOption {
                cluster_type: typ.unwrap_or(ClusterType::Linear),
                cluster_exprs,
            },
        },
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
        |(_, _, point)| AlterTableAction::FlashbackTo { point },
    );

    let set_table_options = map(
        rule! {
            SET ~ OPTIONS ~ "(" ~ #set_table_option ~ ")"
        },
        |(_, _, _, set_options, _)| AlterTableAction::SetOptions { set_options },
    );

    let unset_table_options = map(
        rule! {
            UNSET ~ OPTIONS ~ #unset_source
        },
        |(_, _, targets)| AlterTableAction::UnsetOptions { targets },
    );

    rule!(
        #alter_table_cluster_key
        | #drop_table_cluster_key
        | #rename_table
        | #rename_column
        | #modify_table_comment
        | #add_column
        | #drop_column
        | #modify_column
        | #recluster_table
        | #revert_table
        | #set_table_options
        | #unset_table_options
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
        value(MatchOperation::Delete, rule! { DELETE }),
        map(
            rule! {
                UPDATE ~ SET ~ ^#comma_separated_list1(mutation_update_expr)
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

pub fn literal_duration(i: Input) -> IResult<Duration> {
    let seconds = map(
        rule! {
            #literal_u64 ~ SECONDS
        },
        |(v, _)| Duration::from_secs(v),
    );

    let days = map(
        rule! {
            #literal_u64 ~ DAYS
        },
        |(v, _)| Duration::from_secs(v * 60 * 60 * 24),
    );

    rule!(
        #days
        | #seconds
    )(i)
}

pub fn vacuum_drop_table_option(i: Input) -> IResult<VacuumDropTableOption> {
    alt((map(
        rule! {
            (DRY ~ ^RUN ~ SUMMARY?)? ~ (LIMIT ~ #literal_u64)?
        },
        |(opt_dry_run, opt_limit)| VacuumDropTableOption {
            dry_run: opt_dry_run.map(|dry_run| dry_run.2.is_some()),
            limit: opt_limit.map(|(_, limit)| limit as usize),
        },
    ),))(i)
}

pub fn vacuum_table_option(i: Input) -> IResult<VacuumTableOption> {
    alt((map(
        rule! {
            (DRY ~ ^RUN ~ SUMMARY?)?
        },
        |opt_dry_run| VacuumTableOption {
            dry_run: opt_dry_run.map(|dry_run| dry_run.2.is_some()),
        },
    ),))(i)
}

pub fn task_sql_block(i: Input) -> IResult<TaskSql> {
    let single_statement = map(
        rule! {
            #statement
        },
        |stmt| {
            let sql = format!("{}", stmt.stmt);
            TaskSql::SingleStatement(sql)
        },
    );
    let task_block = map(
        rule! {
            BEGIN
            ~ #semicolon_terminated_list1(statement_body)
            ~ END
        },
        |(_, stmts, _)| {
            let sql = stmts
                .iter()
                .map(|stmt| format!("{}", stmt))
                .collect::<Vec<String>>();
            TaskSql::ScriptBlock(sql)
        },
    );
    alt((single_statement, task_block))(i)
}

pub fn alter_task_option(i: Input) -> IResult<AlterTaskOptions> {
    let suspend = map(
        rule! {
             SUSPEND
        },
        |_| AlterTaskOptions::Suspend,
    );
    let resume = map(
        rule! {
             RESUME
        },
        |_| AlterTaskOptions::Resume,
    );
    let modify_as = map(
        rule! {
             MODIFY ~ AS ~ #task_sql_block
        },
        |(_, _, sql)| AlterTaskOptions::ModifyAs(sql),
    );
    let modify_when = map(
        rule! {
             MODIFY ~ WHEN ~ #expr
        },
        |(_, _, expr)| AlterTaskOptions::ModifyWhen(expr),
    );
    let add_after = map(
        rule! {
             ADD ~ AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, _, after)| AlterTaskOptions::AddAfter(after),
    );
    let remove_after = map(
        rule! {
             REMOVE ~ AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, _, after)| AlterTaskOptions::RemoveAfter(after),
    );

    let set = map(
        rule! {
             SET
             ~ #alter_task_set_option*
             ~ #set_table_option?
        },
        |(_, task_set_options, session_opts)| {
            let mut set = AlterTaskOptions::Set {
                session_parameters: session_opts,
                warehouse: None,
                schedule: None,
                suspend_task_after_num_failures: None,
                comments: None,
                error_integration: None,
            };
            for opt in task_set_options {
                set.apply_opt(opt);
            }
            set
        },
    );
    let unset = map(
        rule! {
             UNSET ~ WAREHOUSE
        },
        |_| AlterTaskOptions::Unset { warehouse: true },
    );
    rule!(
        #suspend
        | #resume
        | #modify_as
        | #set
        | #unset
        | #modify_when
        | #add_after
        | #remove_after
    )(i)
}

pub fn alter_pipe_option(i: Input) -> IResult<AlterPipeOptions> {
    let set = map(
        rule! {
             SET
             ~ ( PIPE_EXECUTION_PAUSED ~ "=" ~ #literal_bool )?
             ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(_, execution_parsed, comment)| AlterPipeOptions::Set {
            execution_paused: execution_parsed.map(|(_, _, paused)| paused),
            comments: comment.map(|(_, _, comment)| comment),
        },
    );
    let refresh = map(
        rule! {
             REFRESH
             ~ ( PREFIX ~ "=" ~ #literal_string )?
             ~ ( MODIFIED_AFTER ~ "=" ~ #literal_string )?
        },
        |(_, prefix, modified_after)| AlterPipeOptions::Refresh {
            prefix: prefix.map(|(_, _, prefix)| prefix),
            modified_after: modified_after.map(|(_, _, modified_after)| modified_after),
        },
    );
    rule!(
        #set
        | #refresh
    )(i)
}

pub fn task_warehouse_option(i: Input) -> IResult<WarehouseOptions> {
    alt((map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)?
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

pub fn assign_nodes_list(i: Input) -> IResult<Vec<(Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            ASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)?
        },
        |(_, node_size, _, node_group)| (node_group.map(|(_, x)| x), node_size),
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })(i)
}

pub fn assign_warehouse_nodes_list(i: Input) -> IResult<Vec<(Identifier, Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            ASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)? ~ FOR ~ #ident
        },
        |(_, node_size, _, node_group, _, cluster)| {
            (cluster, node_group.map(|(_, x)| x), node_size)
        },
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })(i)
}

pub fn unassign_warehouse_nodes_list(i: Input) -> IResult<Vec<(Identifier, Option<String>, u64)>> {
    let nodes_list = map(
        rule! {
            UNASSIGN ~ #literal_u64 ~ NODES ~ (FROM ~ #option_to_string)? ~ FOR ~ #ident
        },
        |(_, node_size, _, node_group, _, cluster)| {
            (cluster, node_group.map(|(_, x)| x), node_size)
        },
    );

    map(comma_separated_list1(nodes_list), |opts| {
        opts.into_iter().collect()
    })(i)
}

pub fn warehouse_cluster_option(i: Input) -> IResult<BTreeMap<String, String>> {
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(k, _, v)| (k, v),
    );
    map(comma_separated_list1(option), |opts| {
        opts.into_iter()
            .map(|(k, v)| (k.name.to_lowercase(), v.clone()))
            .collect()
    })(i)
}

pub fn task_schedule_option(i: Input) -> IResult<ScheduleOptions> {
    let interval = map(
        rule! {
             #literal_u64 ~ MINUTE
        },
        |(mins, _)| ScheduleOptions::IntervalSecs(mins * 60, 0),
    );
    let cron_expr = map(
        rule! {
            USING ~ CRON ~ #literal_string ~ #literal_string?
        },
        |(_, _, expr, timezone)| ScheduleOptions::CronExpression(expr, timezone),
    );
    let interval_sec = map(
        rule! {
             #literal_u64 ~ SECOND
        },
        |(secs, _)| ScheduleOptions::IntervalSecs(secs, 0),
    );
    let interval_millis = map(
        rule! {
             #literal_u64 ~ MILLISECOND
        },
        |(millis, _)| ScheduleOptions::IntervalSecs(0, millis),
    );
    rule!(
        #interval
        | #cron_expr
        | #interval_sec
        | #interval_millis
    )(i)
}

pub fn kill_target(i: Input) -> IResult<KillTarget> {
    alt((
        value(KillTarget::Query, rule! { QUERY }),
        value(KillTarget::Connection, rule! { CONNECTION }),
    ))(i)
}

pub fn priority(i: Input) -> IResult<Priority> {
    alt((
        value(Priority::LOW, rule! { LOW }),
        value(Priority::MEDIUM, rule! { MEDIUM }),
        value(Priority::HIGH, rule! { HIGH }),
    ))(i)
}

pub fn action(i: Input) -> IResult<SystemAction> {
    let mut backtrace = map(
        rule! {
             #switch ~ EXCEPTION_BACKTRACE
        },
        |(switch, _)| SystemAction::Backtrace(switch),
    );
    // add other system action type here
    rule!(
        #backtrace
    )(i)
}

pub fn switch(i: Input) -> IResult<bool> {
    alt((
        value(true, rule! { ENABLE }),
        value(false, rule! { DISABLE }),
    ))(i)
}

pub fn cluster_type(i: Input) -> IResult<ClusterType> {
    alt((
        value(ClusterType::Linear, rule! { LINEAR }),
        value(ClusterType::Hilbert, rule! { HILBERT }),
    ))(i)
}

pub fn limit_where(i: Input) -> IResult<ShowLimit> {
    map(
        rule! {
            WHERE ~ #expr
        },
        |(_, selection)| ShowLimit::Where {
            selection: Box::new(selection),
        },
    )(i)
}

pub fn limit_like(i: Input) -> IResult<ShowLimit> {
    map(
        rule! {
            LIKE ~ #literal_string
        },
        |(_, pattern)| ShowLimit::Like { pattern },
    )(i)
}

pub fn show_limit(i: Input) -> IResult<ShowLimit> {
    rule!(
        #limit_like
        | #limit_where
    )(i)
}

pub fn show_options(i: Input) -> IResult<ShowOptions> {
    map(
        rule! {
            #show_limit? ~ ( LIMIT ~ ^#literal_u64 )?
        },
        |(show_limit, opt_limit)| ShowOptions {
            show_limit,
            limit: opt_limit.map(|(_, limit)| limit),
        },
    )(i)
}

pub fn table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! {
           ( #ident ~ "=" ~ #option_to_string )*
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
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(k, _, v)| (k, v),
    );
    map(comma_separated_list1(option), |opts| {
        opts.into_iter()
            .map(|(k, v)| (k.name.to_lowercase(), v.clone()))
            .collect()
    })(i)
}

pub fn option_to_string(i: Input) -> IResult<String> {
    let bool_to_string = |i| map(literal_bool, |v| v.to_string())(i);

    rule!(
        #bool_to_string
        | #parameter_to_string
    )(i)
}

pub fn engine(i: Input) -> IResult<Engine> {
    let engine = alt((
        value(Engine::Null, rule! { NULL }),
        value(Engine::Memory, rule! { MEMORY }),
        value(Engine::Fuse, rule! { FUSE }),
        value(Engine::View, rule! { VIEW }),
        value(Engine::Random, rule! { RANDOM }),
        value(Engine::Iceberg, rule! { ICEBERG }),
        value(Engine::Delta, rule! { DELTA }),
    ));

    map(
        rule! {
            ENGINE ~ ^"=" ~ ^#engine
        },
        |(_, _, engine)| engine,
    )(i)
}

pub fn database_engine(i: Input) -> IResult<DatabaseEngine> {
    value(DatabaseEngine::Default, rule! { DEFAULT })(i)
}

pub fn create_database_option(i: Input) -> IResult<CreateDatabaseOption> {
    let mut create_db_engine = map(
        rule! {
            ENGINE ~  ^"=" ~ ^#database_engine
        },
        |(_, _, option)| CreateDatabaseOption::DatabaseEngine(option),
    );

    rule!(
        #create_db_engine
    )(i)
}

pub fn catalog_type(i: Input) -> IResult<CatalogType> {
    alt((
        value(CatalogType::Default, rule! { DEFAULT }),
        value(CatalogType::Hive, rule! { HIVE }),
        value(CatalogType::Iceberg, rule! { ICEBERG }),
    ))(i)
}

pub fn user_option(i: Input) -> IResult<UserOptionItem> {
    let tenant_setting = value(UserOptionItem::TenantSetting(true), rule! { TENANTSETTING });
    let no_tenant_setting = value(
        UserOptionItem::TenantSetting(false),
        rule! { NOTENANTSETTING },
    );
    let default_role_option = map(
        rule! {
            DEFAULT_ROLE ~ ^"=" ~ ^#role_name
        },
        |(_, _, role)| UserOptionItem::DefaultRole(role),
    );
    let set_network_policy = map(
        rule! {
            SET ~ NETWORK ~ ^POLICY ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, _, policy)| UserOptionItem::SetNetworkPolicy(policy),
    );
    let unset_network_policy = map(
        rule! {
            UNSET ~ NETWORK ~ ^POLICY
        },
        |(_, _, _)| UserOptionItem::UnsetNetworkPolicy,
    );
    let set_disabled_option = map(
        rule! {
            DISABLED ~ ^"=" ~ #literal_bool
        },
        |(_, _, disabled)| UserOptionItem::Disabled(disabled),
    );
    let set_password_policy = map(
        rule! {
            SET ~ PASSWORD ~ ^POLICY ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, _, policy)| UserOptionItem::SetPasswordPolicy(policy),
    );
    let unset_password_policy = map(
        rule! {
            UNSET ~ PASSWORD ~ ^POLICY
        },
        |(_, _, _)| UserOptionItem::UnsetPasswordPolicy,
    );
    let must_change_password = map(
        rule! {
            MUST_CHANGE_PASSWORD ~ ^"=" ~ ^#literal_bool
        },
        |(_, _, val)| UserOptionItem::MustChangePassword(val),
    );

    rule!(
        #tenant_setting
        | #no_tenant_setting
        | #default_role_option
        | #set_network_policy
        | #unset_network_policy
        | #set_password_policy
        | #unset_password_policy
        | #set_disabled_option
        | #must_change_password
    )(i)
}

pub fn user_identity(i: Input) -> IResult<UserIdentity> {
    map(
        rule! {
            #parameter_to_string ~ ( "@" ~ "'%'" )?
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
        map(rule! { EXPIRE ~ ^"=" ~ ^#literal_u64 }, |(_, _, v)| {
            PresignOption::Expire(v)
        }),
        map(
            rule! { CONTENT_TYPE ~ ^"=" ~ ^#literal_string },
            |(_, _, v)| PresignOption::ContentType(v),
        ),
    ))(i)
}

pub fn table_reference_with_alias(i: Input) -> IResult<TableReference> {
    map(
        consumed(rule! {
            #dot_separated_idents_1_to_3 ~ #alias_name?
        }),
        |(span, ((catalog, database, table), alias))| TableReference::Table {
            span: transform_span(span.tokens),
            catalog,
            database,
            table,
            alias: alias.map(|v| TableAlias {
                name: v,
                columns: vec![],
            }),
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        },
    )(i)
}

pub fn table_reference_only(i: Input) -> IResult<TableReference> {
    map(
        consumed(rule! {
            #dot_separated_idents_1_to_3
        }),
        |(span, (catalog, database, table))| TableReference::Table {
            span: transform_span(span.tokens),
            catalog,
            database,
            table,
            alias: None,
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
        },
    )(i)
}

pub fn update_expr(i: Input) -> IResult<UpdateExpr> {
    map(rule! { ( #ident ~ "=" ~ ^#expr ) }, |(name, _, expr)| {
        UpdateExpr { name, expr }
    })(i)
}

pub fn udaf_state_field(i: Input) -> IResult<UDAFStateField> {
    map(
        rule! {
            #ident
            ~ #type_name
            : "`<state name> <type>`"
        },
        |(name, type_name)| UDAFStateField { name, type_name },
    )(i)
}

pub fn udf_script_or_address(i: Input) -> IResult<(String, bool)> {
    let script = map(
        rule! {
            AS ~ ^(#code_string | #literal_string)
        },
        |(_, code)| (code, true),
    );

    let address = map(
        rule! {
            ADDRESS ~ ^"=" ~ ^#literal_string
        },
        |(_, _, address)| (address, false),
    );

    rule!(
        #script: "AS <language_codes>"
        | #address: "ADDRESS=<udf_server_address>"
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

    let udf = map(
        rule! {
            "(" ~ #comma_separated_list0(type_name) ~ ")"
            ~ RETURNS ~ #type_name
            ~ LANGUAGE ~ #ident
            ~ HANDLER ~ ^"=" ~ ^#literal_string
            ~ #udf_script_or_address
        },
        |(_, arg_types, _, _, return_type, _, language, _, _, handler, address_or_code)| {
            if address_or_code.1 {
                UDFDefinition::UDFScript {
                    arg_types,
                    return_type,
                    code: address_or_code.0,
                    handler,
                    language: language.to_string(),
                    // TODO inject runtime_version by user
                    // Now we use fixed runtime version
                    runtime_version: "".to_string(),
                }
            } else {
                UDFDefinition::UDFServer {
                    arg_types,
                    return_type,
                    address: address_or_code.0,
                    handler,
                    language: language.to_string(),
                }
            }
        },
    );

    let udaf = map(
        rule! {
            "(" ~ #comma_separated_list0(type_name) ~ ")"
            ~ STATE ~ "{" ~ #comma_separated_list0(udaf_state_field) ~ "}"
            ~ RETURNS ~ #type_name
            ~ LANGUAGE ~ #ident
            ~ #udf_script_or_address
        },
        |(_, arg_types, _, _, _, state_types, _, _, return_type, _, language, address_or_code)| {
            if address_or_code.1 {
                UDFDefinition::UDAFScript {
                    arg_types,
                    state_fields: state_types,
                    return_type,
                    code: address_or_code.0,
                    language: language.to_string(),
                    // TODO inject runtime_version by user
                    // Now we use fixed runtime version
                    runtime_version: "".to_string(),
                }
            } else {
                UDFDefinition::UDAFServer {
                    arg_types,
                    state_fields: state_types,
                    return_type,
                    address: address_or_code.0,
                    language: language.to_string(),
                }
            }
        },
    );

    rule!(
        #lambda_udf: "AS (<parameter>, ...) -> <definition expr>"
        | #udaf: "(<arg_type>, ...) STATE {<state_field>, ...} RETURNS <return_type> LANGUAGE <language> { ADDRESS=<udf_server_address> | AS <language_codes> } "
        | #udf: "(<arg_type>, ...) RETURNS <return_type> LANGUAGE <language> HANDLER=<handler> { ADDRESS=<udf_server_address> | AS <language_codes> } "

    )(i)
}

pub fn mutation_update_expr(i: Input) -> IResult<MutationUpdateExpr> {
    map(
        rule! { #dot_separated_idents_1_to_2 ~ "=" ~ ^#expr },
        |((table, name), _, expr)| MutationUpdateExpr { table, name, expr },
    )(i)
}

pub fn password_set_options(i: Input) -> IResult<PasswordSetOptions> {
    map(
        rule! {
             ( PASSWORD_MIN_LENGTH ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_LENGTH ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_UPPER_CASE_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_LOWER_CASE_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_NUMERIC_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_SPECIAL_CHARS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MIN_AGE_DAYS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_AGE_DAYS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_MAX_RETRIES ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_LOCKOUT_TIME_MINS ~ Eq ~ ^#literal_u64 )?
             ~ ( PASSWORD_HISTORY ~ Eq ~ ^#literal_u64 )?
             ~ ( COMMENT ~ Eq ~ ^#literal_string)?
        },
        |(
            opt_min_length,
            opt_max_length,
            opt_min_upper_case_chars,
            opt_min_lower_case_chars,
            opt_min_numeric_chars,
            opt_min_special_chars,
            opt_min_age_days,
            opt_max_age_days,
            opt_max_retries,
            opt_lockout_time_mins,
            opt_history,
            opt_comment,
        )| {
            PasswordSetOptions {
                min_length: opt_min_length.map(|opt| opt.2),
                max_length: opt_max_length.map(|opt| opt.2),
                min_upper_case_chars: opt_min_upper_case_chars.map(|opt| opt.2),
                min_lower_case_chars: opt_min_lower_case_chars.map(|opt| opt.2),
                min_numeric_chars: opt_min_numeric_chars.map(|opt| opt.2),
                min_special_chars: opt_min_special_chars.map(|opt| opt.2),
                min_age_days: opt_min_age_days.map(|opt| opt.2),
                max_age_days: opt_max_age_days.map(|opt| opt.2),
                max_retries: opt_max_retries.map(|opt| opt.2),
                lockout_time_mins: opt_lockout_time_mins.map(|opt| opt.2),
                history: opt_history.map(|opt| opt.2),
                comment: opt_comment.map(|opt| opt.2),
            }
        },
    )(i)
}

pub fn password_unset_options(i: Input) -> IResult<PasswordUnSetOptions> {
    map(
        rule! {
             PASSWORD_MIN_LENGTH?
             ~ PASSWORD_MAX_LENGTH?
             ~ PASSWORD_MIN_UPPER_CASE_CHARS?
             ~ PASSWORD_MIN_LOWER_CASE_CHARS?
             ~ PASSWORD_MIN_NUMERIC_CHARS?
             ~ PASSWORD_MIN_SPECIAL_CHARS?
             ~ PASSWORD_MIN_AGE_DAYS?
             ~ PASSWORD_MAX_AGE_DAYS?
             ~ PASSWORD_MAX_RETRIES?
             ~ PASSWORD_LOCKOUT_TIME_MINS?
             ~ PASSWORD_HISTORY?
             ~ COMMENT?
        },
        |(
            opt_min_length,
            opt_max_length,
            opt_min_upper_case_chars,
            opt_min_lower_case_chars,
            opt_min_numeric_chars,
            opt_min_special_chars,
            opt_min_age_days,
            opt_max_age_days,
            opt_max_retries,
            opt_lockout_time_mins,
            opt_history,
            opt_comment,
        )| {
            PasswordUnSetOptions {
                min_length: opt_min_length.is_some(),
                max_length: opt_max_length.is_some(),
                min_upper_case_chars: opt_min_upper_case_chars.is_some(),
                min_lower_case_chars: opt_min_lower_case_chars.is_some(),
                min_numeric_chars: opt_min_numeric_chars.is_some(),
                min_special_chars: opt_min_special_chars.is_some(),
                min_age_days: opt_min_age_days.is_some(),
                max_age_days: opt_max_age_days.is_some(),
                max_retries: opt_max_retries.is_some(),
                lockout_time_mins: opt_lockout_time_mins.is_some(),
                history: opt_history.is_some(),
                comment: opt_comment.is_some(),
            }
        },
    )(i)
}

pub fn alter_password_action(i: Input) -> IResult<AlterPasswordAction> {
    let set_options = map(
        rule! {
           SET ~ #password_set_options
        },
        |(_, set_options)| AlterPasswordAction::SetOptions(set_options),
    );
    let unset_options = map(
        rule! {
           UNSET ~ #password_unset_options
        },
        |(_, unset_options)| AlterPasswordAction::UnSetOptions(unset_options),
    );

    rule!(
        #set_options
        | #unset_options
    )(i)
}

pub fn explain_option(i: Input) -> IResult<ExplainOption> {
    map(
        rule! {
            VERBOSE | LOGICAL | OPTIMIZED | DECORRELATED
        },
        |opt| match &opt.kind {
            VERBOSE => ExplainOption::Verbose,
            LOGICAL => ExplainOption::Logical,
            OPTIMIZED => ExplainOption::Optimized,
            DECORRELATED => ExplainOption::Decorrelated,
            _ => unreachable!(),
        },
    )(i)
}

pub fn create_task_option(i: Input) -> IResult<CreateTaskOption> {
    let warehouse_opt = map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)
        },
        |(_, _, warehouse)| CreateTaskOption::Warehouse(warehouse),
    );
    let schedule_opt = map(
        rule! {
            SCHEDULE ~ "=" ~ #task_schedule_option
        },
        |(_, _, schedule)| CreateTaskOption::Schedule(schedule),
    );
    let after_opt = map(
        rule! {
            AFTER ~ #comma_separated_list0(literal_string)
        },
        |(_, after)| CreateTaskOption::After(after),
    );
    let when_opt = map(
        rule! {
            WHEN ~ #expr
        },
        |(_, expr)| CreateTaskOption::When(expr),
    );
    let suspend_task_after_num_failures_opt = map(
        rule! {
            SUSPEND_TASK_AFTER_NUM_FAILURES ~ "=" ~ #literal_u64
        },
        |(_, _, num)| CreateTaskOption::SuspendTaskAfterNumFailures(num),
    );
    let error_integration_opt = map(
        rule! {
            ERROR_INTEGRATION ~ "=" ~ #literal_string
        },
        |(_, _, integration)| CreateTaskOption::ErrorIntegration(integration),
    );
    let comment_opt = map(
        rule! {
            (COMMENT | COMMENTS) ~ "=" ~ #literal_string
        },
        |(_, _, comment)| CreateTaskOption::Comment(comment),
    );

    map(
        rule! {
            #warehouse_opt
            | #schedule_opt
            | #after_opt
            | #when_opt
            | #suspend_task_after_num_failures_opt
            | #error_integration_opt
            | #comment_opt
        },
        |opt| opt,
    )(i)
}

fn alter_task_set_option(i: Input) -> IResult<AlterTaskSetOption> {
    let warehouse_opt = map(
        rule! {
            (WAREHOUSE  ~ "=" ~ #literal_string)
        },
        |(_, _, warehouse)| AlterTaskSetOption::Warehouse(warehouse),
    );
    let schedule_opt = map(
        rule! {
            SCHEDULE ~ "=" ~ #task_schedule_option
        },
        |(_, _, schedule)| AlterTaskSetOption::Schedule(schedule),
    );
    let suspend_task_after_num_failures_opt = map(
        rule! {
            SUSPEND_TASK_AFTER_NUM_FAILURES ~ "=" ~ #literal_u64
        },
        |(_, _, num)| AlterTaskSetOption::SuspendTaskAfterNumFailures(num),
    );
    let error_integration_opt = map(
        rule! {
            ERROR_INTEGRATION ~ "=" ~ #literal_string
        },
        |(_, _, integration)| AlterTaskSetOption::ErrorIntegration(integration),
    );
    let comment_opt = map(
        rule! {
            (COMMENT | COMMENTS) ~ "=" ~ #literal_string
        },
        |(_, _, comment)| AlterTaskSetOption::Comment(comment),
    );

    map(
        rule! {
            #warehouse_opt
            | #schedule_opt
            | #suspend_task_after_num_failures_opt
            | #error_integration_opt
            | #comment_opt
        },
        |opt| opt,
    )(i)
}

pub fn notification_webhook_options(i: Input) -> IResult<NotificationWebhookOptions> {
    let url_option = map(
        rule! {
            URL ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("url".to_string(), v.to_string()),
    );
    let method_option = map(
        rule! {
            METHOD ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("method".to_string(), v.to_string()),
    );
    let auth_option = map(
        rule! {
            AUTHORIZATION_HEADER ~ "=" ~ #literal_string
        },
        |(_, _, v)| ("authorization_header".to_string(), v.to_string()),
    );

    map(
        rule! { ((
        #url_option
        | #method_option
        | #auth_option) ~ ","?)* },
        |opts| {
            NotificationWebhookOptions::from_iter(
                opts.iter().map(|((k, v), _)| (k.to_uppercase(), v.clone())),
            )
        },
    )(i)
}

pub fn notification_webhook_clause(i: Input) -> IResult<NotificationWebhookOptions> {
    map(
        rule! { WEBHOOK ~ ^"=" ~ ^"(" ~ ^#notification_webhook_options ~ ^")" },
        |(_, _, _, opts, _)| opts,
    )(i)
}

pub fn alter_notification_options(i: Input) -> IResult<AlterNotificationOptions> {
    let enabled = map(
        rule! {
            SET ~ ENABLED ~ ^"=" ~ #literal_bool
        },
        |(_, _, _, enabled)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::enabled(enabled))
        },
    );
    let webhook = map(
        rule! {
            SET ~ #notification_webhook_clause
        },
        |(_, webhook)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::webhook_opts(webhook))
        },
    );
    let comment = map(
        rule! {
            SET ~ (COMMENT | COMMENTS) ~ ^"=" ~ #literal_string
        },
        |(_, _, _, comment)| {
            AlterNotificationOptions::Set(AlterNotificationSetOptions::comments(comment))
        },
    );
    map(
        rule! {
            #enabled
            | #webhook
            | #comment
        },
        |opts| opts,
    )(i)
}

pub fn virtual_column(i: Input) -> IResult<VirtualColumn> {
    map(
        rule! {
            #expr ~ #alias_name?
        },
        |(expr, alias)| VirtualColumn {
            expr: Box::new(expr),
            alias,
        },
    )(i)
}

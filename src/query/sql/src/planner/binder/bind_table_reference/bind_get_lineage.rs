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

use databend_common_ast::Span;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::parser::parse_table_ref;
use databend_common_base::runtime::block_on;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::i64_value;
use databend_common_catalog::table_args::string_value;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::BindContext;
use crate::Binder;
use crate::Planner;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::plans::Plan;

const GET_LINEAGE_FUNC: &str = "get_lineage";
const DEFAULT_DISTANCE: u8 = 5;
const MAX_DISTANCE: u8 = 5;

#[derive(Clone, Copy)]
enum ObjectDomain {
    Table,
    View,
    Stage,
    Column,
}

impl ObjectDomain {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TABLE" => Ok(Self::Table),
            "VIEW" => Ok(Self::View),
            "STAGE" => Ok(Self::Stage),
            "COLUMN" => Ok(Self::Column),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported object_domain '{other}', expected TABLE, VIEW, STAGE, or COLUMN"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Table => "TABLE",
            Self::View => "VIEW",
            Self::Stage => "STAGE",
            Self::Column => "COLUMN",
        }
    }

    fn system_relation(self) -> &'static str {
        match self {
            Self::Table => "system.tables",
            Self::View => "system.views",
            Self::Stage => "system.stages",
            Self::Column => unreachable!("COLUMN resolves its containing object separately"),
        }
    }
}

#[derive(Clone, Copy)]
enum QueryDirection {
    /// Walk from the queried target toward the source objects that feed it.
    Upstream,
    /// Walk from the queried source toward the target objects that consume it.
    Downstream,
}

impl QueryDirection {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "UPSTREAM" => Ok(Self::Upstream),
            "DOWNSTREAM" => Ok(Self::Downstream),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported direction '{other}', expected UPSTREAM or DOWNSTREAM"
            ))),
        }
    }

    fn edge_view(self) -> &'static str {
        match self {
            Self::Upstream => "system_history.lineage_by_target",
            Self::Downstream => "system_history.lineage_by_source",
        }
    }

    fn start_lookup_sql(
        self,
        object_type: &str,
        system_relation: &str,
        catalog: &str,
        database: &str,
        object_name: &str,
    ) -> String {
        if object_type == "STAGE" {
            return format!(
                "SELECT concat('STAGE::NAME::', name) AS lookup_key FROM system.stages WHERE name = {object_name}"
            );
        }
        let start_filter = format!(
            r#"FROM {system_relation}
        WHERE dropped_on IS NULL
          AND catalog = {catalog}
          AND database = {database}
          AND name = {object_name}"#
        );
        format!(
            r#"SELECT concat('{object_type}::ID::', to_string(table_id)) AS lookup_key
        {start_filter}
        UNION ALL
        SELECT concat('{object_type}::NAME::', catalog, '.', database, '.', name) AS lookup_key
        {start_filter}"#
        )
    }
}

struct GetLineageArgs {
    object_name: String,
    object_domain: ObjectDomain,
    direction: QueryDirection,
    distance: u8,
}

impl GetLineageArgs {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(GET_LINEAGE_FUNC, None)?;
        if !(args.len() == 3 || args.len() == 4) {
            return Err(ErrorCode::BadArguments(format!(
                "{GET_LINEAGE_FUNC} requires 3 or 4 positioned arguments: object_name, object_domain, direction[, distance]"
            )));
        }

        let distance = if args.len() == 4 {
            let distance = i64_value(&args[3])?;
            if !(1..=MAX_DISTANCE as i64).contains(&distance) {
                return Err(ErrorCode::BadArguments(format!(
                    "distance must be an integer in the range [1, {MAX_DISTANCE}]"
                )));
            }
            distance as u8
        } else {
            DEFAULT_DISTANCE
        };

        Ok(Self {
            object_name: string_value(&args[0])?,
            object_domain: ObjectDomain::parse(&string_value(&args[1])?)?,
            direction: QueryDirection::parse(&string_value(&args[2])?)?,
            distance,
        })
    }
}

impl Binder {
    /// Expand GET_LINEAGE into a bounded sequence of CTE levels during binding so traversal stays
    /// in the caller's query plan and each level can remove duplicate semantic edges.
    pub(super) fn bind_get_lineage(
        &mut self,
        span: &Span,
        alias: &Option<TableAlias>,
        table_args: &TableArgs,
    ) -> Result<(SExpr, BindContext)> {
        let args = GetLineageArgs::parse(table_args).map_err(|err| err.set_span(*span))?;
        let sql = match args.object_domain {
            ObjectDomain::Column => {
                let (table_name, column_name) = split_column_name(&args.object_name)?;
                let (catalog, database, object_name) =
                    self.parse_lineage_object_name(&table_name)?;
                let column_name = self.parse_lineage_column_name(&column_name)?;
                build_column_lineage_sql(&args, &catalog, &database, &object_name, &column_name)
            }
            ObjectDomain::Table | ObjectDomain::View | ObjectDomain::Stage => {
                let (catalog, database, object_name) =
                    self.parse_lineage_object_name(&args.object_name)?;
                build_object_lineage_sql(&args, &catalog, &database, &object_name)
            }
        };

        let statement = Planner::new(self.ctx.clone()).parse_sql(&sql)?.statement;
        let binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        )
        .with_subquery_executor(self.subquery_executor.clone());
        let plan = block_on(binder.bind(&statement))?;
        let Plan::Query {
            s_expr,
            bind_context,
            ..
        } = plan
        else {
            return Err(ErrorCode::Internal(
                "GET_LINEAGE traversal query returned no result set",
            ));
        };

        let mut bind_context = *bind_context;
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }
        Ok((*s_expr, bind_context))
    }

    fn parse_lineage_object_name(&self, value: &str) -> Result<(String, String, String)> {
        let value = value.trim();
        if value.is_empty() {
            return Err(ErrorCode::BadArguments("object_name must not be empty"));
        }

        let table_ref = parse_table_ref(value, self.dialect).map_err(|err| {
            ErrorCode::BadArguments(format!("invalid object_name '{value}': {}", err.1))
        })?;
        let catalog = table_ref
            .catalog
            .map(|ident| normalize_identifier(&ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = table_ref
            .database
            .map(|ident| normalize_identifier(&ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let object_name = normalize_identifier(&table_ref.table, &self.name_resolution_ctx).name;
        Ok((catalog, database, object_name))
    }

    fn parse_lineage_column_name(&self, value: &str) -> Result<String> {
        let column_ref = parse_table_ref(value, self.dialect).map_err(|err| {
            ErrorCode::BadArguments(format!("invalid column name '{value}': {}", err.1))
        })?;
        if column_ref.catalog.is_some() || column_ref.database.is_some() {
            return Err(ErrorCode::BadArguments(format!(
                "invalid column name '{value}'"
            )));
        }
        Ok(normalize_identifier(&column_ref.table, &self.name_resolution_ctx).name)
    }
}

fn build_object_lineage_sql(
    args: &GetLineageArgs,
    catalog: &str,
    database: &str,
    object_name: &str,
) -> String {
    let object_type = args.object_domain.as_str();
    let system_relation = args.object_domain.system_relation();
    let edge_view = args.direction.edge_view();
    let catalog = quote_sql_string(catalog);
    let database = quote_sql_string(database);
    let object_name = quote_sql_string(object_name);
    let start_lookup = args.direction.start_lookup_sql(
        object_type,
        system_relation,
        &catalog,
        &database,
        &object_name,
    );
    let mut levels = Vec::with_capacity(args.distance as usize);
    levels.push(format!(
        r#"level_1 AS (
    SELECT 1::UInt8 AS distance, edge.edge_key, edge.next_lookup_keys,
           edge.query_id, edge.event_time, edge.query_kind, edge.lineage_kind,
           edge.column_lineage_hash, edge.source_object_type, edge.source_object_name,
           edge.source_object_id, edge.target_object_type, edge.target_object_name,
           edge.target_object_id
    FROM {edge_view} AS edge
    INNER JOIN ({start_lookup}) AS start ON edge.match_key = start.lookup_key
    QUALIFY row_number() OVER (
        PARTITION BY edge.edge_key
        ORDER BY edge.event_time DESC NULLS LAST, edge.query_id DESC NULLS LAST,
                 edge.lineage_kind DESC, edge.column_lineage_hash DESC
    ) = 1
)"#
    ));
    for level in 2..=args.distance {
        let previous = level - 1;
        levels.push(format!(
            r#"level_{level} AS (
    SELECT {level}::UInt8 AS distance, edge.edge_key, edge.next_lookup_keys,
           edge.query_id, edge.event_time, edge.query_kind, edge.lineage_kind,
           edge.column_lineage_hash, edge.source_object_type, edge.source_object_name,
           edge.source_object_id, edge.target_object_type, edge.target_object_name,
           edge.target_object_id
    FROM level_{previous} AS current
    INNER JOIN {edge_view} AS edge
        ON contains(current.next_lookup_keys, edge.match_key)
    QUALIFY row_number() OVER (
        PARTITION BY edge.edge_key
        ORDER BY edge.event_time DESC NULLS LAST, edge.query_id DESC NULLS LAST,
                 edge.lineage_kind DESC, edge.column_lineage_hash DESC
    ) = 1
)"#
        ));
    }
    let all_levels = (1..=args.distance)
        .map(|level| format!("SELECT * FROM level_{level}"))
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");

    format!(
        r#"WITH {levels}
SELECT
    distance::Int32 AS distance,
    source_object_type AS source_object_domain,
    source_object_name,
    NULL::STRING AS source_column_name,
    target_object_type AS target_object_domain,
    target_object_name,
    NULL::STRING AS target_column_name,
    'ACTIVE' AS target_status,
    to_string(json_object(
        'query_id', query_id,
        'query_kind', query_kind,
        'lineage_kind', lineage_kind,
        'event_time', event_time
    )) AS process
FROM ({all_levels}) AS lineage_walk
QUALIFY row_number() OVER (
    PARTITION BY edge_key
    ORDER BY distance, event_time DESC NULLS LAST, query_id DESC NULLS LAST,
             lineage_kind DESC, column_lineage_hash DESC
) = 1"#,
        levels = levels.join(",\n"),
    )
}

fn build_column_lineage_sql(
    args: &GetLineageArgs,
    catalog: &str,
    database: &str,
    object_name: &str,
    column_name: &str,
) -> String {
    let edge_view = args.direction.edge_view();
    let catalog = quote_sql_string(catalog);
    let database = quote_sql_string(database);
    let object_name = quote_sql_string(object_name);
    let column_name = quote_sql_string(column_name);
    let start_object = format!(
        r#"start_object AS (
    SELECT
        object_type,
        table_id,
        column_id,
        column_name,
        [
            concat(object_type, '::ID::', to_string(table_id)),
            concat(object_type, '::NAME::', catalog, '.', database, '.', object_name)
        ] AS lookup_keys
    FROM (
        SELECT
            objects.object_type,
            objects.catalog,
            objects.database,
            objects.object_name,
            objects.table_id,
            columns.column_id,
            columns.name AS column_name
        FROM (
            SELECT 'TABLE' AS object_type, catalog, database, name AS object_name, table_id
            FROM system.tables
            WHERE dropped_on IS NULL
            UNION ALL
            SELECT 'VIEW' AS object_type, catalog, database, name AS object_name, table_id
            FROM system.views
            WHERE dropped_on IS NULL
        ) AS objects
        INNER JOIN system.columns AS columns
            ON columns.database = objects.database
            AND columns.table = objects.object_name
        WHERE objects.catalog = {catalog}
          AND objects.database = {database}
          AND objects.object_name = {object_name}
          AND columns.name = {column_name}
    )
    QUALIFY row_number() OVER (
        PARTITION BY object_type, table_id, column_name
        ORDER BY column_id DESC NULLS LAST
    ) = 1
)"#
    );
    let levels = (1..=args.distance)
        .map(|level| build_column_level_sql(args.direction, edge_view, level))
        .collect::<Vec<_>>();
    let all_levels = (1..=args.distance)
        .map(|level| format!("SELECT * FROM level_{level}"))
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");

    format!(
        r#"WITH {start_object},
{levels}
SELECT
    distance::Int32 AS distance,
    source_object_type AS source_object_domain,
    source_object_name,
    source_column_name,
    target_object_type AS target_object_domain,
    target_object_name,
    target_column_name,
    'ACTIVE' AS target_status,
    to_string(json_object(
        'query_id', query_id,
        'query_kind', query_kind,
        'lineage_kind', lineage_kind,
        'event_time', event_time
    )) AS process
FROM ({all_levels}) AS lineage_walk
QUALIFY row_number() OVER (
    PARTITION BY edge_key, source_column_name, target_column_name
    ORDER BY distance, event_time DESC NULLS LAST, query_id DESC NULLS LAST,
             lineage_kind DESC, column_lineage_hash DESC
) = 1"#,
        levels = levels.join(",\n"),
    )
}

fn build_column_level_sql(direction: QueryDirection, edge_view: &str, level: u8) -> String {
    let (current_relation, current_lookup_keys, current_column_id, current_column_name) =
        if level == 1 {
            (
                "start_object".to_string(),
                "current.lookup_keys",
                "to_string(current.column_id)",
                "current.column_name",
            )
        } else {
            (
                format!("level_{}", level - 1),
                "current.next_lookup_keys",
                "current.next_column_id",
                "current.next_column_name",
            )
        };
    let next_column_name =
        "iff(coalesce(mapped.next_catalog_type, '') = 'DEFAULT', next_col.name, mapped.column_ref)";
    let (source_column, target_column) = match direction {
        QueryDirection::Upstream => (next_column_name, "mapped.current_column_name"),
        QueryDirection::Downstream => ("mapped.current_column_name", next_column_name),
    };

    format!(
        r#"level_{level} AS (
    SELECT {level}::UInt8 AS distance, mapped.edge_key, mapped.next_lookup_keys,
           iff(coalesce(mapped.next_catalog_type, '') = 'DEFAULT', to_string(next_col.column_id), mapped.column_ref) AS next_column_id,
           {next_column_name} AS next_column_name,
           mapped.query_id, mapped.event_time, mapped.query_kind, mapped.lineage_kind,
           mapped.column_lineage_hash, mapped.source_object_type, mapped.source_object_name,
           mapped.source_object_id, {source_column} AS source_column_name,
           mapped.target_object_type, mapped.target_object_name, mapped.target_object_id,
           {target_column} AS target_column_name
    FROM (
        SELECT edge.*,
               {current_column_name} AS current_column_name,
               unnest(coalesce(get(
                   edge.column_map,
                   iff(
                       edge.current_column_address_kind = 'ID',
                       {current_column_id},
                       {current_column_name}
                   )
               ), []::ARRAY(STRING))) AS column_ref
        FROM {current_relation} AS current
        INNER JOIN {edge_view} AS edge
            ON contains({current_lookup_keys}, edge.match_key)
    ) AS mapped
    LEFT JOIN system.columns AS next_col
        ON coalesce(mapped.next_catalog_type, '') = 'DEFAULT'
        AND next_col.database = mapped.next_object_database
        AND next_col.table = mapped.next_object_short_name
        AND (
            (mapped.next_column_address_kind = 'ID'
                AND to_string(next_col.column_id) = mapped.column_ref)
            OR (mapped.next_column_address_kind = 'NAME'
                AND next_col.name = mapped.column_ref)
        )
    WHERE coalesce(mapped.next_catalog_type, '') != 'DEFAULT' OR next_col.name IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY mapped.edge_key, source_column_name, target_column_name
        ORDER BY mapped.event_time DESC NULLS LAST, mapped.query_id DESC NULLS LAST,
                 mapped.lineage_kind DESC, mapped.column_lineage_hash DESC
    ) = 1
)"#
    )
}

fn split_column_name(input: &str) -> Result<(String, String)> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ErrorCode::BadArguments(
            "column object_name must not be empty",
        ));
    }

    let Some(index) = last_unquoted_dot(input) else {
        return Err(ErrorCode::BadArguments(
            "column object_name must be qualified by table name",
        ));
    };
    let table = input[..index].trim();
    let column = input[index + 1..].trim();
    if table.is_empty() || column.is_empty() {
        return Err(ErrorCode::BadArguments(
            "column object_name must be in table.column format",
        ));
    }
    Ok((table.to_string(), column.to_string()))
}

fn last_unquoted_dot(input: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut last_dot = None;
    let mut iter = input.char_indices().peekable();
    while let Some((index, ch)) = iter.next() {
        match ch {
            '"' => {
                if in_quote && matches!(iter.peek(), Some((_, '"'))) {
                    iter.next();
                } else {
                    in_quote = !in_quote;
                }
            }
            '.' if !in_quote => last_dot = Some(index),
            _ => {}
        }
    }
    last_dot
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    #[test]
    fn test_generated_object_lineage_sql_parses() -> Result<()> {
        for direction in [QueryDirection::Upstream, QueryDirection::Downstream] {
            let object_args = GetLineageArgs {
                object_name: "db.table".to_string(),
                object_domain: ObjectDomain::Table,
                direction,
                distance: 5,
            };
            parse(&build_object_lineage_sql(
                &object_args,
                "default",
                "db",
                "table",
            ))?;
        }
        Ok(())
    }

    #[test]
    fn test_generated_column_lineage_sql_parses() -> Result<()> {
        // Query runtimes use a 20 MiB stack. Match that here because the maximum-depth generated
        // query exceeds Rust's much smaller default test-thread stack in debug builds.
        std::thread::Builder::new()
            .stack_size(20 * 1024 * 1024)
            .spawn(|| {
                for direction in [QueryDirection::Upstream, QueryDirection::Downstream] {
                    let column_args = GetLineageArgs {
                        object_name: "db.table.column".to_string(),
                        object_domain: ObjectDomain::Column,
                        direction,
                        distance: 5,
                    };
                    let sql =
                        build_column_lineage_sql(&column_args, "default", "db", "table", "column");
                    assert!(sql.contains("level_5 AS"));
                    assert!(sql.contains("unnest(coalesce(get("));
                    assert!(sql.contains("edge.column_map"));
                    assert!(sql.contains("mapped.next_catalog_type, '') != 'DEFAULT'"));
                    assert!(sql.contains("LEFT JOIN system.columns AS next_col"));
                    assert!(!sql.contains("WITH RECURSIVE"));
                    parse(&sql)?;
                }
                Ok(())
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn test_split_quoted_column_name() -> Result<()> {
        assert_eq!(
            split_column_name(r#"db."table.with.dot"."column.with.dot""#)?,
            (
                r#"db."table.with.dot""#.to_string(),
                r#""column.with.dot""#.to_string(),
            )
        );
        Ok(())
    }

    fn parse(sql: &str) -> Result<()> {
        let tokens = tokenize_sql(sql)?;
        parse_sql(&tokens, Dialect::PostgreSQL)?;
        Ok(())
    }
}

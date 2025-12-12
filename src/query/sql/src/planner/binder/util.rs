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

use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::parser::Dialect;
use databend_common_ast::span::merge_span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::Binder;
use crate::NameResolutionContext;
use crate::NameResolutionSuggest;

/// Ident name can not contain ' or "
/// Forbidden ' or " in UserName and RoleName, to prevent Meta injection problem
pub fn illegal_ident_name(ident_name: &str) -> bool {
    ident_name
        .chars()
        .any(|c| c == '\'' || c == '\"' || c == '\u{000C}' || c == '\u{0008}')
}

impl Binder {
    // Find all recursive cte scans
    #[allow(clippy::only_used_in_recursion)]
    pub fn count_r_cte_scan(
        &mut self,
        expr: &SExpr,
        cte_scan_names: &mut Vec<String>,
        cte_types: &mut Vec<DataType>,
    ) -> Result<()> {
        match expr.plan() {
            RelOperator::RecursiveCteScan(plan) => {
                cte_scan_names.push(plan.table_name.clone());
                if cte_types.is_empty() {
                    cte_types.extend(plan.fields.iter().map(|f| f.data_type().clone()));
                }
            }
            // Each recursive step in a recursive query generates new rows, and these rows are used for the next recursion.
            // Each step depends on the results of the previous step, so it's essential to ensure that the result set is built incrementally.
            // These operators need to operate on the entire result set,
            // which is incompatible with the way a recursive query incrementally builds the result set.
            RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Window(_)
            | RelOperator::Mutation(_)
            | RelOperator::MutationSource(_)
            | RelOperator::CompactBlock(_) => {
                return Err(ErrorCode::SyntaxException(format!(
                    "{:?} is not allowed in recursive cte",
                    expr.plan().rel_op()
                )));
            }
            _ => {
                let arity = expr.plan().arity();
                for i in 0..arity {
                    self.count_r_cte_scan(expr.child(i)?, cte_scan_names, cte_types)?;
                }
            }
        }
        Ok(())
    }
}

pub struct TableIdentifier {
    catalog: Identifier,
    database: Identifier,
    table: Identifier,
    table_ref: Option<Identifier>,
    table_alias: Option<TableAlias>,
    dialect: Dialect,
    name_resolution_ctx: NameResolutionContext,
}

impl TableIdentifier {
    pub fn new(
        binder: &Binder,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        table: &Identifier,
        table_ref: &Option<Identifier>,
        table_alias: &Option<TableAlias>,
    ) -> TableIdentifier {
        // Use the common normalization logic to handle MySQL-style identifiers.
        // This handles the special case: `db`.``.`table` -> database="db", catalog=current
        let (catalog_name, database_name) =
            binder.normalize_mysql_catalog_database_pair(catalog, database);

        let Binder {
            ctx,
            name_resolution_ctx,
            dialect,
            ..
        } = binder;

        // Reconstruct Identifier objects from the normalized names
        let catalog = match catalog_name {
            Some(name) => Identifier {
                span: catalog.as_ref().and_then(|c| c.span),
                name,
                quote: Some(dialect.default_ident_quote()),
                ident_type: IdentifierType::None,
            },
            None => Identifier {
                span: None,
                name: ctx.get_current_catalog(),
                quote: Some(dialect.default_ident_quote()),
                ident_type: IdentifierType::None,
            },
        };
        let database = match database_name {
            Some(name) => Identifier {
                span: database.as_ref().and_then(|d| d.span),
                name,
                quote: Some(dialect.default_ident_quote()),
                ident_type: IdentifierType::None,
            },
            None => Identifier {
                span: None,
                name: ctx.get_current_database(),
                quote: Some(dialect.default_ident_quote()),
                ident_type: IdentifierType::None,
            },
        };
        let database = Identifier {
            span: merge_span(catalog.span, database.span),
            ..database
        };
        let table = Identifier {
            span: merge_span(database.span, table.span),
            name: table.name.clone(),
            ..*table
        };
        TableIdentifier {
            catalog,
            database,
            table,
            table_ref: table_ref.clone(),
            table_alias: table_alias.clone(),
            dialect: *dialect,
            name_resolution_ctx: name_resolution_ctx.clone(),
        }
    }

    pub fn catalog_name(&self) -> String {
        normalize_identifier(&self.catalog, &self.name_resolution_ctx).name
    }

    pub fn database_name(&self) -> String {
        normalize_identifier(&self.database, &self.name_resolution_ctx).name
    }

    pub fn table_name(&self) -> String {
        normalize_identifier(&self.table, &self.name_resolution_ctx).name
    }

    pub fn table_ref_name(&self) -> Option<String> {
        self.table_ref
            .as_ref()
            .map(|v| normalize_identifier(v, &self.name_resolution_ctx).name)
    }

    pub fn table_name_alias(&self) -> Option<String> {
        self.table_alias.as_ref().map(|table_alias| {
            normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name
        })
    }

    pub fn not_found_suggest_error(&self, err: ErrorCode) -> ErrorCode {
        let Self {
            catalog,
            database,
            table,
            ..
        } = self;
        match err.code() {
            ErrorCode::UNKNOWN_DATABASE => {
                let error_message = match self.name_resolution_ctx.not_found_suggest(database) {
                    Some(NameResolutionSuggest::Quoted) => {
                        format!(
                            "Unknown database {catalog}.{database} (unquoted). Did you mean {} (quoted)?",
                            QuotedIdent(&database.name, self.dialect.default_ident_quote())
                        )
                    }
                    Some(NameResolutionSuggest::Unquoted) => {
                        format!(
                            "Unknown database {catalog}.{database} (quoted). Did you mean {} (unquoted)?",
                            &database.name
                        )
                    }
                    None => format!("Unknown database {catalog}.{database} ."),
                };
                ErrorCode::UnknownDatabase(error_message).set_span(database.span)
            }
            ErrorCode::UNKNOWN_TABLE => {
                let error_message = match self.name_resolution_ctx.not_found_suggest(table) {
                    Some(NameResolutionSuggest::Quoted) => {
                        format!(
                            "Unknown table {catalog}.{database}.{table} (unquoted). Did you mean {} (quoted)?",
                            QuotedIdent(&table.name, self.dialect.default_ident_quote())
                        )
                    }
                    Some(NameResolutionSuggest::Unquoted) => {
                        format!(
                            "Unknown table {catalog}.{database}.{table} (quoted). Did you mean {} (unquoted)?",
                            &table.name
                        )
                    }
                    None => format!("Unknown table {catalog}.{database}.{table} ."),
                };
                ErrorCode::UnknownTable(error_message).set_span(table.span)
            }
            _ => err,
        }
    }
}

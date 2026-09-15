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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use chrono_tz::Tz;
use databend_common_ast::ast::Hint;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SetType;
use databend_common_ast::ast::SetValues;
use databend_common_ast::ast::Settings;
use databend_common_ast::ast::Statement;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::cast_scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use log::warn;
use parking_lot::RwLock;

use super::BindContext;
use super::Metadata;
use super::NameResolutionContext;
use super::TypeChecker;
use super::binder::wrap_cast;
use crate::MetadataRef;

const SQL_DIALECT_SETTING: &str = "sql_dialect";

#[derive(Default)]
struct StatementSettingsCollector {
    // Even an empty SETTINGS clause suppresses SET_VAR hints.
    has_settings_clause: bool,
    settings: Option<Settings>,
    hints: Vec<Hint>,
    copy_disable_variant_check: bool,
}

impl StatementSettingsCollector {
    fn scan(stmt: &Statement) -> Self {
        let mut collector = Self::default();
        stmt.walk(&mut collector).unwrap();
        collector
    }

    fn collect_statement(&mut self, stmt: &Statement) {
        let hint = match stmt {
            Statement::StatementWithSettings { settings, .. } => {
                self.has_settings_clause = true;
                if self.settings.is_none() {
                    self.settings.clone_from(settings);
                }
                None
            }
            Statement::CopyIntoTable(stmt) => {
                self.copy_disable_variant_check |= stmt.options.disable_variant_check;
                stmt.hints.as_ref()
            }
            Statement::CopyIntoLocation(stmt) => stmt.hints.as_ref(),
            Statement::Insert(stmt) => stmt.hints.as_ref(),
            Statement::Replace(stmt) => stmt.hints.as_ref(),
            Statement::MergeInto(stmt) => stmt.hints.as_ref(),
            Statement::Delete(stmt) => stmt.hints.as_ref(),
            Statement::Update(stmt) => stmt.hints.as_ref(),
            _ => None,
        };
        if let Some(hint) = hint {
            self.hints.push(hint.clone());
        }
    }

    fn collect_select_stmt(&mut self, stmt: &SelectStmt) {
        if let Some(hint) = &stmt.hints {
            self.hints.push(hint.clone());
        }
    }
}

impl Visitor for StatementSettingsCollector {
    fn visit_statement(&mut self, stmt: &Statement) -> std::result::Result<VisitControl, !> {
        self.collect_statement(stmt);
        Ok(VisitControl::Continue)
    }

    fn visit_select_stmt(&mut self, stmt: &SelectStmt) -> std::result::Result<VisitControl, !> {
        self.collect_select_stmt(stmt);
        Ok(VisitControl::Continue)
    }
}

pub fn apply_statement_settings(ctx: Arc<dyn TableContext>, stmt: &Statement) -> Result<()> {
    let collector = StatementSettingsCollector::scan(stmt);

    let mut resolved = if collector.has_settings_clause {
        if let Some(settings) = collector.settings {
            let metadata = Arc::new(RwLock::new(Metadata::default()));
            resolve_settings(ctx.clone(), metadata, &settings)?
        } else {
            HashMap::new()
        }
    } else if !collector.hints.is_empty() {
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        for hint in collector.hints {
            match resolve_hint(ctx.clone(), metadata.clone(), &hint) {
                // Apply each group before resolving the next so dependent expressions
                // see earlier settings. Later groups override earlier groups in AST walk order.
                Ok(group) => ctx.get_shared_settings().set_batch_settings(&group, true)?,
                Err(error) => {
                    warn!("[SQL-PLANNER] Failed to resolve optimize hint {hint:?}: {error:?}");
                }
            }
        }
        HashMap::new()
    } else {
        HashMap::new()
    };

    if collector.copy_disable_variant_check {
        resolved.insert("disable_variant_check".to_string(), "1".to_string());
    }

    ctx.get_shared_settings()
        .set_batch_settings(&resolved, true)
}

fn resolve_settings(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    settings: &Settings,
) -> Result<HashMap<String, String>> {
    let Settings {
        set_type: SetType::SettingsQuery,
        identifiers,
        values,
    } = settings
    else {
        return Err(ErrorCode::BadArguments("Only support query level setting"));
    };

    let SetValues::Expr(values) = values else {
        return Err(ErrorCode::SemanticError(
            "query setting value must be scalar",
        ));
    };

    let mut bind_context = BindContext::new();
    let name_resolution_ctx = NameResolutionContext::default();
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;
    let func_ctx = ctx.get_function_context()?;
    let mut resolved = HashMap::new();
    for (identifier, value) in identifiers.iter().zip(values) {
        let variable = identifier.to_string().to_lowercase();
        if ignore_current_query_sql_dialect(ctx.as_ref(), &variable) {
            continue;
        }

        let (scalar, _) = *type_checker.resolve(value.as_ref())?;
        let expr = scalar.as_expr()?;
        let (folded, _) = ConstantFolder::fold(Cow::Owned(expr), &func_ctx, &BUILTIN_FUNCTIONS);
        let Expr::Constant(Constant { scalar, .. }) = folded.into_owned() else {
            return Err(ErrorCode::SemanticError("value must be constant value"));
        };
        let scalar = cast_scalar(None, scalar, &DataType::String, &BUILTIN_FUNCTIONS)?;
        let value = scalar.into_string().unwrap();
        validate_timezone(&variable, &value)?;
        // The first occurrence wins within this group.
        resolved.entry(variable).or_insert(value);
    }
    Ok(resolved)
}

fn resolve_hint(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    hint: &Hint,
) -> Result<HashMap<String, String>> {
    let mut bind_context = BindContext::new();
    let name_resolution_ctx = NameResolutionContext::default();
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx.clone(),
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;
    let func_ctx = ctx.get_function_context()?;
    let mut resolved = HashMap::new();
    for item in &hint.hints_list {
        let variable = item.name.name.to_lowercase();
        if ignore_current_query_sql_dialect(ctx.as_ref(), &variable) {
            continue;
        }

        let (scalar, _) = *type_checker.resolve(&item.expr)?;
        let scalar = wrap_cast(&scalar, &DataType::String);
        let expr = scalar.as_expr()?;
        let (folded, _) = ConstantFolder::fold(Cow::Owned(expr), &func_ctx, &BUILTIN_FUNCTIONS);
        let Expr::Constant(Constant { scalar, .. }) = folded.into_owned() else {
            return Err(ErrorCode::SemanticError("hint value must be a constant"));
        };
        let value = scalar.into_string().unwrap();
        validate_timezone(&variable, &value)?;
        // The first occurrence wins within this group.
        resolved.entry(variable).or_insert(value);
    }
    Ok(resolved)
}

fn ignore_current_query_sql_dialect(ctx: &dyn TableContext, variable: &str) -> bool {
    if variable != SQL_DIALECT_SETTING {
        return false;
    }

    ctx.push_warning(
        "Query-level setting 'sql_dialect' is ignored because it cannot change the SQL dialect \
         of the current query"
            .to_string(),
    );
    true
}

fn validate_timezone(variable: &str, value: &str) -> Result<()> {
    if variable == "timezone" {
        let timezone = value.trim_matches(['\'', '"']);
        timezone
            .parse::<Tz>()
            .map_err(|_| ErrorCode::InvalidTimezone(format!("Invalid Timezone: {value:?}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;

    use super::StatementSettingsCollector;

    fn scan(sql: &str) -> StatementSettingsCollector {
        let tokens = tokenize_sql(sql).unwrap();
        let (stmt, _) = parse_sql(&tokens, Dialect::Experimental).unwrap();
        StatementSettingsCollector::scan(&stmt)
    }

    #[test]
    fn test_scan_nested_set_var_hints_in_walk_order() {
        let collector = scan(
            "SELECT /*+ SET_VAR(max_threads=1) */ * \
             FROM (SELECT /*+ SET_VAR(max_threads=2) */ 1)",
        );

        assert!(!collector.has_settings_clause);
        assert_eq!(collector.hints.len(), 2);
        assert_eq!(collector.hints[0].hints_list[0].name.name, "max_threads");
        assert_eq!(collector.hints[1].hints_list[0].name.name, "max_threads");
        assert_eq!(collector.hints[0].hints_list[0].expr.to_string(), "1");
        assert_eq!(collector.hints[1].hints_list[0].expr.to_string(), "2");
    }

    #[test]
    fn test_settings_clause_suppresses_all_collected_hints() {
        let collector = scan(
            "SETTINGS (max_threads = 3) \
             SELECT /*+ SET_VAR(max_threads=1) */ * \
             FROM (SELECT /*+ SET_VAR(max_threads=2) */ 1)",
        );

        assert!(collector.has_settings_clause);
        assert!(collector.settings.is_some());
        assert_eq!(collector.hints.len(), 2);
    }

    #[test]
    fn test_copy_option_is_collected_separately_from_hint() {
        let collector = scan(
            "COPY /*+ SET_VAR(disable_variant_check=0) */ INTO t \
             FROM 'fs:///tmp/data.json' \
             FILE_FORMAT = (TYPE = NDJSON) DISABLE_VARIANT_CHECK = true",
        );

        assert_eq!(collector.hints.len(), 1);
        assert!(collector.copy_disable_variant_check);
    }
}

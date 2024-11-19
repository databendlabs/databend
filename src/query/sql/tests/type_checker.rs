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

use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::ast::Expr;
use databend_common_catalog::table_context::TableContext;
use databend_common_sql::BindContext;
use databend_common_sql::MetadataRef;
use databend_query::sql::NameResolutionContext;
use databend_common_sql::TypeChecker;

#[test]
fn test_resolve_function_call_name() {
    let mut bind_context = BindContext::default();
    let table_context = Arc::new(TableContext::new());
    let name_resolution_ctx = NameResolutionContext {
        unquoted_ident_case_sensitive: true,
        quoted_ident_case_sensitive: true,
        deny_column_reference: false,
    };;
    let metadata = MetadataRef::default();
    let aliases = vec![];

    let type_checker = TypeChecker::try_create(
        &mut bind_context,
        table_context,
        &name_resolution_ctx,
        metadata,
        &aliases,
        false,
    );

    let tokens = tokenize_sql("SELECT VERSION();").unwrap();
    let mut expr = parse_expr(&tokens, Dialect::Mysql).unwrap();

    if let Ok(mut checker) = type_checker {
        let result = checker.resolve(&expr);

        assert!(result.is_ok(), "Function resolution failed");
    }
}

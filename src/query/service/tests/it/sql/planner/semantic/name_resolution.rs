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

use databend_common_ast::ast::Identifier;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::walk_expr_mut;
use databend_common_ast::Dialect;
use databend_query::sql::normalize_identifier;
use databend_query::sql::IdentifierNormalizer;
use databend_query::sql::NameResolutionContext;

#[test]
fn test_normalize_identifier_default() {
    let ctx = NameResolutionContext::default();

    {
        // Unquoted
        let ident = Identifier {
            name: "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
                .to_string(),
            quote: None,
            span: None,
        };
        let norm_name = normalize_identifier(&ident, &ctx).name;
        assert_eq!(
            norm_name,
            "foobar 这是一个标识符 これは識別子です это идентификатор dies ist eine kennung"
        );
    }

    {
        // Quoted
        let ident = Identifier {
            name: "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
                .to_string(),
            quote: Some('"'),
            span: None,
        };
        let norm_name = normalize_identifier(&ident, &ctx).name;
        assert_eq!(
            norm_name,
            "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
        );
    }
}

#[test]
fn test_normalize_identifier_quoted_case_insensitive() {
    let ctx = NameResolutionContext {
        unquoted_ident_case_sensitive: false,
        quoted_ident_case_sensitive: false,
        deny_column_reference: false,
    };

    {
        // Quoted
        let ident = Identifier {
            name: "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
                .to_string(),
            quote: Some('"'),
            span: None,
        };
        let norm_name = normalize_identifier(&ident, &ctx).name;
        assert_eq!(
            norm_name,
            "foobar 这是一个标识符 これは識別子です это идентификатор dies ist eine kennung"
        );
    }
}

#[test]
fn test_normalize_identifier_unquoted_case_sensitive() {
    let ctx = NameResolutionContext {
        unquoted_ident_case_sensitive: true,
        quoted_ident_case_sensitive: true,
        deny_column_reference: false,
    };

    {
        // Unquoted
        let ident = Identifier {
            name: "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
                .to_string(),
            quote: None,
            span: None,
        };
        let norm_name = normalize_identifier(&ident, &ctx).name;
        assert_eq!(
            norm_name,
            "FooBar 这是一个标识符 これは識別子です Это ИДЕНТификатор Dies ist eine Kennung"
        );
    }
}

#[test]
fn test_normalize_identifiers_in_expr() {
    let tokens = tokenize_sql("exists(select func(\"T\".A+1) as B)").unwrap();
    let mut expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();

    let ctx = NameResolutionContext::default();
    let mut normalizer = IdentifierNormalizer { ctx: &ctx };

    walk_expr_mut(&mut normalizer, &mut expr);

    assert_eq!(
        format!("{:#}", expr),
        "EXISTS (SELECT func((\"T\".a + 1)) AS b)".to_string()
    );
}

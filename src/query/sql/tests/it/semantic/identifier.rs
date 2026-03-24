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

use std::io::Write;

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::quote::display_ident;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::Result;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::normalize_identifier;

use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn write_normalized_identifier(
    file: &mut impl Write,
    label: &str,
    ident: &Identifier,
    context: &NameResolutionContext,
) -> Result<()> {
    let normalized = normalize_identifier(ident, context);
    let quote = normalized.quote.unwrap_or('-');
    writeln!(
        file,
        "{label}: name={} quote={quote}",
        normalized.name
    )?;
    Ok(())
}

fn write_context(file: &mut impl Write, context: &NameResolutionContext) -> Result<()> {
    writeln!(
        file,
        "context: unquoted_case_sensitive={} quoted_case_sensitive={} deny_column_reference={}",
        context.unquoted_ident_case_sensitive,
        context.quoted_ident_case_sensitive,
        context.deny_column_reference
    )?;
    Ok(())
}

#[test]
fn test_identifier_semantics() -> Result<()> {
    let mut file = open_golden_file("semantic", "identifier.txt")?;

    write_case_title(
        &mut file,
        "quoted_cte_round_trip_preserves_quotes",
        "AST round-tripping should keep quoted identifiers in the rendered SQL.",
    )?;
    let sql = r#"WITH "QuotedCTE" AS (SELECT id, "Value" FROM "QuotedTable" WHERE id = 1) SELECT * FROM "QuotedCTE""#;
    let tokens = tokenize_sql(sql)?;
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
    writeln!(file, "input_sql: {sql}")?;
    writeln!(file, "round_trip_sql: {stmt}")?;
    writeln!(file)?;

    write_case_title(
        &mut file,
        "normalization_keeps_quote_information",
        "Identifier normalization may fold case, but it must preserve whether the identifier was quoted.",
    )?;
    let case_insensitive_context = NameResolutionContext {
        unquoted_ident_case_sensitive: false,
        quoted_ident_case_sensitive: false,
        deny_column_reference: false,
    };
    write_context(&mut file, &case_insensitive_context)?;
    let quoted_ident = Identifier {
        span: Default::default(),
        name: "MixedCase".to_string(),
        quote: Some('"'),
        ident_type: Default::default(),
    };
    let unquoted_ident = Identifier {
        span: Default::default(),
        name: "MixedCase".to_string(),
        quote: None,
        ident_type: Default::default(),
    };
    write_normalized_identifier(
        &mut file,
        "quoted",
        &quoted_ident,
        &case_insensitive_context,
    )?;
    write_normalized_identifier(
        &mut file,
        "unquoted",
        &unquoted_ident,
        &case_insensitive_context,
    )?;
    writeln!(file)?;

    write_case_title(
        &mut file,
        "repeated_normalization_preserves_quotedness",
        "Running normalization across multiple phases should not strip quote information from quoted identifiers.",
    )?;
    let mixed_sensitivity_context = NameResolutionContext {
        unquoted_ident_case_sensitive: false,
        quoted_ident_case_sensitive: true,
        deny_column_reference: false,
    };
    let original = Identifier {
        span: Default::default(),
        name: "MixedCaseIdentifier".to_string(),
        quote: Some('"'),
        ident_type: Default::default(),
    };
    let first = normalize_identifier(&original, &case_insensitive_context);
    let second = normalize_identifier(&first, &mixed_sensitivity_context);
    let third = normalize_identifier(&second, &case_insensitive_context);
    writeln!(
        file,
        "phase1: name={} quote={}",
        first.name,
        first.quote.unwrap_or('-')
    )?;
    writeln!(
        file,
        "phase2: name={} quote={}",
        second.name,
        second.quote.unwrap_or('-')
    )?;
    writeln!(
        file,
        "phase3: name={} quote={}",
        third.name,
        third.quote.unwrap_or('-')
    )?;
    writeln!(file)?;

    write_case_title(
        &mut file,
        "display_ident_quotes_unicode_uppercase_when_needed",
        "Rendering must keep quotes when dropping them would make reparsing change identifier case.",
    )?;
    let quoted_case_sensitive_context = NameResolutionContext {
        unquoted_ident_case_sensitive: false,
        quoted_ident_case_sensitive: true,
        deny_column_reference: false,
    };
    write_context(&mut file, &quoted_case_sensitive_context)?;
    let rendered = display_ident(
        "Ж",
        false,
        quoted_case_sensitive_context.quoted_ident_case_sensitive,
        Dialect::PostgreSQL,
    );
    let reparsed_unquoted = Identifier {
        span: Default::default(),
        name: "Ж".to_string(),
        quote: None,
        ident_type: Default::default(),
    };
    writeln!(file, "rendered: {rendered}")?;
    write_normalized_identifier(
        &mut file,
        "reparsed_unquoted",
        &reparsed_unquoted,
        &quoted_case_sensitive_context,
    )?;
    writeln!(file)?;

    Ok(())
}

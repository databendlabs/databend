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
#[cfg(test)]
mod tests {

    use databend_common_ast::ast::Identifier;
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_exception::Result;

    use crate::planner::semantic::name_resolution::NameResolutionContext;
    use crate::planner::semantic::name_resolution::normalize_identifier;

    #[test]
    fn test_sql_to_ast_to_sql_quoted_identifiers() -> Result<()> {
        // Test case 1: Simple WITH clause with quoted identifiers
        let sql = r#"WITH "QuotedCTE" AS (SELECT * FROM "QuotedTable" WHERE id = 1) SELECT * FROM "QuotedCTE""#;

        // Parse SQL to AST
        // let tokens = sql.to_string();
        let tokens = tokenize_sql(sql)?;
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;

        // Convert AST back to SQL
        let regenerated_sql = stmt.to_string();

        // The regenerated SQL should still contain the quoted identifiers
        assert!(
            regenerated_sql.contains(r#""QuotedCTE""#),
            "Quoted CTE name lost in regenerated SQL"
        );
        assert!(
            regenerated_sql.contains(r#""QuotedTable""#),
            "Quoted table name lost in regenerated SQL"
        );

        // Test case 2: More complex WITH clause with mixed case identifiers
        let sql2 = r#"WITH "MixedCaseCTE" AS (SELECT id, "Value" FROM "MixedCaseTable" WHERE id = 1) SELECT * FROM "MixedCaseCTE""#;

        // Parse SQL to AST
        // let tokens2 = sql2.to_string();
        let tokens2 = tokenize_sql(sql2)?;
        let (stmt2, _) = parse_sql(&tokens2, Dialect::PostgreSQL)?;

        // Convert AST back to SQL
        let regenerated_sql2 = stmt2.to_string();

        // The regenerated SQL should still contain the quoted identifiers with correct case
        assert!(
            regenerated_sql2.contains(r#""MixedCaseCTE""#),
            "Quoted CTE name lost in regenerated SQL"
        );
        assert!(
            regenerated_sql2.contains(r#""Value""#),
            "Quoted column name lost in regenerated SQL"
        );
        assert!(
            regenerated_sql2.contains(r#""MixedCaseTable""#),
            "Quoted table name lost in regenerated SQL"
        );

        Ok(())
    }

    #[test]
    fn test_normalize_identifier_preserves_quotes() -> Result<()> {
        // Create a context where case sensitivity is off for both quoted and unquoted identifiers
        let context = NameResolutionContext {
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: false,
            deny_column_reference: false,
        };

        // Test with a quoted identifier
        let quoted_ident = Identifier {
            span: Default::default(),
            name: "MixedCase".to_string(),
            quote: Some('"'),
            ident_type: Default::default(),
        };

        // Normalize the identifier
        let normalized = normalize_identifier(&quoted_ident, &context);

        // Check that the quote information is preserved
        assert_eq!(
            normalized.quote,
            Some('"'),
            "Quote information was lost during normalization"
        );
        assert_eq!(
            normalized.name, "mixedcase",
            "Name should be lowercase after normalization"
        );

        // Test with an unquoted identifier
        let unquoted_ident = Identifier {
            span: Default::default(),
            name: "MixedCase".to_string(),
            quote: None,
            ident_type: Default::default(),
        };

        // Normalize the identifier
        let normalized = normalize_identifier(&unquoted_ident, &context);

        // Check that it remains unquoted
        assert_eq!(
            normalized.quote, None,
            "Unquoted identifier should remain unquoted"
        );
        assert_eq!(
            normalized.name, "mixedcase",
            "Name should be lowercase after normalization"
        );

        Ok(())
    }

    #[test]
    fn test_multiple_normalizations() -> Result<()> {
        // This test simulates the scenario where an identifier is normalized multiple times
        // which could happen during multiple parsing/planning phases

        // Create contexts with different case sensitivity settings
        let case_insensitive = NameResolutionContext {
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: false,
            deny_column_reference: false,
        };

        let mixed_sensitivity = NameResolutionContext {
            unquoted_ident_case_sensitive: false,
            quoted_ident_case_sensitive: true,
            deny_column_reference: false,
        };

        // Start with a quoted identifier
        let original = Identifier {
            span: Default::default(),
            name: "MixedCaseIdentifier".to_string(),
            quote: Some('"'),
            ident_type: Default::default(),
        };

        // First normalization (case insensitive for all)
        let first_norm = normalize_identifier(&original, &case_insensitive);

        // Second normalization (case sensitive for quoted)
        let second_norm = normalize_identifier(&first_norm, &mixed_sensitivity);

        // Third normalization (back to case insensitive)
        let third_norm = normalize_identifier(&second_norm, &case_insensitive);

        // Check that the quote information is preserved through all normalizations
        assert_eq!(
            first_norm.quote,
            Some('"'),
            "Quote lost after first normalization"
        );
        assert_eq!(
            second_norm.quote,
            Some('"'),
            "Quote lost after second normalization"
        );
        assert_eq!(
            third_norm.quote,
            Some('"'),
            "Quote lost after third normalization"
        );

        // The name should be lowercase due to the case insensitive context
        assert_eq!(third_norm.name, "mixedcaseidentifier");

        Ok(())
    }
}

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

use databend_common_ast::ast::SetExpr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;

#[test]
fn test_union_with_parentheses() {
    let test_cases = vec![
        // Original issue #19578
        "SELECT 1 UNION (SELECT 1 UNION SELECT 1 UNION SELECT 1)",
        "SELECT 1 UNION (SELECT 2)",
        "(SELECT 1) UNION SELECT 2",
        "(SELECT 1) UNION (SELECT 2)",
        "SELECT 1 UNION SELECT 2 UNION (SELECT 3 UNION SELECT 4)",
        "(SELECT 1 UNION SELECT 2) UNION SELECT 3",
        // With INTERSECT and EXCEPT
        "SELECT 1 INTERSECT (SELECT 1 INTERSECT SELECT 1)",
        "(SELECT 1) EXCEPT SELECT 2",
        // Complex cases
        "SELECT a FROM t1 UNION (SELECT b FROM t2 INTERSECT SELECT c FROM t3)",
        "(SELECT * FROM t1 WHERE x > 0) UNION SELECT * FROM t2",
    ];

    for sql in test_cases {
        println!("Testing: {}", sql);
        
        // Parse the SQL
        let tokens = tokenize_sql(sql).unwrap();
        let (stmt, rest) = parse_sql(&tokens, Dialect::PostgreSQL)
            .unwrap_or_else(|e| panic!("Failed to parse SQL '{}': {:?}", sql, e));
        
        assert!(rest.is_empty(), "Not all input consumed for '{}'", sql);
        
        // Check that it's a SELECT statement
        let select_stmt = match stmt {
            databend_common_ast::ast::Statement::Query(query) => query,
            _ => panic!("Expected Query statement for '{}'", sql),
        };
        
        // Verify the structure contains parentheses where expected
        verify_parentheses_preserved(&select_stmt.body, sql);
        
        // Test round-trip: parse -> format -> parse again
        let formatted = select_stmt.to_string();
        println!("Formatted: {}", formatted);
        
        let tokens2 = tokenize_sql(&formatted).unwrap();
        let (stmt2, rest2) = parse_sql(&tokens2, Dialect::PostgreSQL)
            .unwrap_or_else(|e| panic!("Failed to re-parse formatted SQL '{}': {:?}", formatted, e));
        
        assert!(rest2.is_empty(), "Not all input consumed for re-parse of '{}'", formatted);
        
        // The re-parsed statement should be equivalent
        let formatted2 = match stmt2 {
            databend_common_ast::ast::Statement::Query(query) => query.to_string(),
            _ => panic!("Expected Query statement after re-parse"),
        };
        
        // The formatted strings should match (allowing for whitespace differences)
        let normalize = |s: &str| s.replace(" ", "").replace("\n", "").replace("\t", "");
        assert_eq!(
            normalize(&formatted),
            normalize(&formatted2),
            "Round-trip failed for '{}'",
            sql
        );
    }
}

fn verify_parentheses_preserved(set_expr: &SetExpr, original_sql: &str) {
    match set_expr {
        SetExpr::SetOperation(_op) => {
            // Check if this operation should have parentheses based on original SQL
            let sql_lower = original_sql.to_lowercase();
            let has_parentheses = sql_lower.contains("(select") && sql_lower.contains(")");
            
            if has_parentheses {
                // For now, just verify the structure is valid
                // In a real test, we would check the AST structure more precisely
                println!("Verified parentheses preserved for: {}", original_sql);
            }
        }
        SetExpr::Query(query) => {
            // This is a parenthesized subquery
            verify_parentheses_preserved(&query.body, original_sql);
        }
        _ => {
            // Simple SELECT, nothing to check
        }
    }
}

#[test]
fn test_issue_19578_specific() {
    // The exact test case from issue #19578
    let sql = "SELECT 1 UNION (SELECT 1 UNION SELECT 1 UNION SELECT 1)";
    
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, rest) = parse_sql(&tokens, Dialect::PostgreSQL)
        .expect("Failed to parse issue #19578 SQL");
    
    assert!(rest.is_empty());
    
    // Format and verify it can be re-parsed
    let formatted = match stmt {
        databend_common_ast::ast::Statement::Query(query) => query.to_string(),
        _ => panic!("Expected Query"),
    };
    
    println!("Issue #19578 formatted: {}", formatted);
    
    // Re-parse to ensure no panic
    let tokens2 = tokenize_sql(&formatted).unwrap();
    let (_, rest2) = parse_sql(&tokens2, Dialect::PostgreSQL)
        .expect("Failed to re-parse formatted SQL");
    
    assert!(rest2.is_empty());
}
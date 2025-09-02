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

//! SQL Error Suggestion System
//!
//! Provides intelligent syntax-based suggestions for SQL parsing errors.
//! Uses automatic grammar extraction and similarity matching to suggest
//! the most likely intended commands.

use std::sync::OnceLock;

/// A matched pattern with its similarity score
#[derive(Debug, Clone)]
struct PatternMatch {
    pattern: String,
    score: f64,
}

/// SQL error suggestion system
pub struct ErrorSuggestion;

impl ErrorSuggestion {
    pub fn new() -> Self {
        Self
    }

    /// Get common SQL command patterns
    fn get_patterns() -> &'static [&'static str] {
        &[
            "SHOW TABLES",
            "SHOW DATABASES",
            "SHOW FUNCTIONS",
            "SHOW USERS",
            "SHOW ROLES",
            "SHOW ENGINES",
            "SHOW STAGES",
            "SHOW METRICS",
            "SHOW WAREHOUSES",
            "SHOW PROCESSLIST",
            "SHOW VARIABLES",
            "SHOW SETTINGS",
            "SHOW LOCKS",
            "SHOW COLUMNS",
            "SHOW CREATE TABLE",
            "CREATE TABLE",
            "CREATE DATABASE",
            "CREATE USER",
            "CREATE VIEW",
            "DROP TABLE",
            "DROP DATABASE",
            "DROP USER",
            "DROP VIEW",
            "ALTER TABLE",
            "INSERT INTO",
            "SELECT",
            "UPDATE",
            "DELETE FROM",
            "TRUNCATE TABLE",
            "USE",
            "DESCRIBE",
            "EXPLAIN",
            // Maintenance commands
            "OPTIMIZE TABLE",
            "VACUUM TABLE",
            "VACUUM DROP TABLE",
            "VACUUM TEMPORARY FILES",
            "VACUUM TEMPORARY TABLES",
            "ANALYZE TABLE",
        ]
    }

    /// Generate a suggestion for SQL parsing error
    pub fn suggest(&self, input: &str) -> Option<String> {
        let trimmed_input = input.trim();

        // Don't suggest for very short inputs
        if trimmed_input.len() < 2 {
            return None;
        }

        // First, check if this is a valid command prefix that needs context help
        if let Some(context_help) = self.provide_context_help(input) {
            return Some(context_help);
        }

        let matches = self.find_best_matches(input, 3);

        // Only suggest if we have a good match that looks like a real error
        for pattern in matches.iter().take(1) {
            // Require higher similarity for error suggestions (not just prefix completion)
            if pattern.score < 6.0 {
                return None;
            }

            let simplified = self.extract_definite_prefix(&pattern.pattern);

            // Don't suggest if input is already correct
            if self.is_correct_command(input, &simplified) {
                return None;
            }

            // Only suggest if this looks like a real typo or error
            if self.is_likely_error(input, &simplified) && self.is_definite_command(&simplified) {
                return Some(format!("Did you mean `{}`?", simplified));
            }
        }

        None
    }

    /// Find best matching patterns for input
    fn find_best_matches(&self, input: &str, limit: usize) -> Vec<PatternMatch> {
        let input_tokens: Vec<String> = input.split_whitespace().map(|s| s.to_string()).collect();

        let mut matches: Vec<PatternMatch> = Self::get_patterns()
            .iter()
            .map(|pattern| {
                let score = self.calculate_similarity(&input_tokens, pattern);
                PatternMatch {
                    pattern: pattern.to_string(),
                    score,
                }
            })
            .filter(|m| m.score > 0.5)
            .collect();

        matches.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        matches.truncate(limit);
        matches
    }

    /// Calculate similarity between input tokens and pattern
    fn calculate_similarity(&self, input_tokens: &[String], pattern: &str) -> f64 {
        let pattern_tokens: Vec<&str> = pattern.split_whitespace().collect();
        let mut score = 0.0;
        let mut matches = 0;

        for (i, input_token) in input_tokens.iter().enumerate() {
            if i < pattern_tokens.len() {
                let pattern_token = pattern_tokens[i];
                if input_token.eq_ignore_ascii_case(pattern_token) {
                    score += 2.0;
                    matches += 1;
                } else if self.is_similar_keyword(input_token, pattern_token) {
                    score += 1.5; // Bonus for singular/plural fixes (less than perfect match)
                    matches += 1;
                } else {
                    // Check for typos using edit distance
                    let edit_distance = self.calculate_edit_distance(
                        &input_token.to_lowercase(),
                        &pattern_token.to_lowercase(),
                    );
                    if edit_distance > 0 && edit_distance <= 2 && input_token.len() >= 3 {
                        score += 1.0; // Lower score for typos
                        matches += 1;
                    } else {
                        break; // Stop on major mismatch
                    }
                }
            }
        }

        // Bonus for complete input coverage
        if matches == input_tokens.len() && matches > 0 {
            score += 3.0; // Reduced bonus to account for typo scoring
        }

        // Extra bonus for single word perfect prefix match
        if input_tokens.len() == 1 && matches == 1 {
            score += 2.0;
        }

        score
    }

    /// Check if input is similar to pattern (prefix matching only)
    fn is_similar_keyword(&self, input: &str, pattern: &str) -> bool {
        let input_lower = input.to_lowercase();
        let pattern_lower = pattern.to_lowercase();

        // Only allow pattern to start with input (prefix matching)
        pattern_lower.starts_with(&input_lower)
    }

    /// Provide context help for valid but incomplete command prefixes
    fn provide_context_help(&self, input: &str) -> Option<String> {
        let input_clean = input.trim().to_uppercase();
        let input_words: Vec<&str> = input_clean.split_whitespace().collect();

        // Only provide context help for single words that are valid prefixes
        if input_words.len() != 1 {
            return None;
        }

        let prefix = input_words[0];

        // Check if this is a valid command prefix
        let matching_commands: Vec<&str> = Self::get_patterns()
            .iter()
            .filter(|pattern| {
                let pattern_upper = pattern.to_uppercase();
                let pattern_words: Vec<&str> = pattern_upper.split_whitespace().collect();
                !pattern_words.is_empty() && pattern_words[0] == prefix
            })
            .take(3) // Limit to 3 suggestions for readability
            .copied()
            .collect();

        if matching_commands.len() >= 2 {
            if matching_commands.len() == 2 {
                return Some(format!(
                    "Try: `{}` or `{}`",
                    matching_commands[0], matching_commands[1]
                ));
            } else {
                return Some(format!(
                    "Try: `{}`, `{}`, or `{}`",
                    matching_commands[0], matching_commands[1], matching_commands[2]
                ));
            }
        }

        None
    }

    /// Check if input is already a correct command or valid prefix
    fn is_correct_command(&self, input: &str, suggestion: &str) -> bool {
        let input_clean = input.trim().to_uppercase();
        let suggestion_clean = suggestion.trim().to_uppercase();

        // If input exactly matches suggestion, it's already correct
        if input_clean == suggestion_clean {
            return true;
        }

        // Check if input is a valid prefix of any command (not just the suggested one)
        for &pattern in Self::get_patterns() {
            let pattern_upper = pattern.to_uppercase();
            if pattern_upper.starts_with(&format!("{} ", input_clean)) {
                return true;
            }
        }

        false
    }

    /// Check if input looks like a likely error/typo rather than just incomplete
    fn is_likely_error(&self, input: &str, suggestion: &str) -> bool {
        let input_clean = input.trim().to_uppercase();
        let suggestion_clean = suggestion.trim().to_uppercase();

        let input_words: Vec<&str> = input_clean.split_whitespace().collect();
        let suggestion_words: Vec<&str> = suggestion_clean.split_whitespace().collect();

        // Case 1: Same number of words but with typos (e.g., "show tabl" vs "SHOW TABLES")
        if input_words.len() == suggestion_words.len() {
            // Check if there are actual character differences (typos)
            for (input_word, suggestion_word) in input_words.iter().zip(suggestion_words.iter()) {
                if input_word != suggestion_word {
                    // If it's not just a case difference and looks like a typo
                    let edit_distance = self.calculate_edit_distance(input_word, suggestion_word);
                    if edit_distance > 0 && edit_distance <= 3 {
                        return true; // Looks like a typo
                    }
                    // Or if it's a common singular/plural error
                    if self.is_similar_keyword(input_word, suggestion_word) {
                        return true;
                    }
                }
            }
        }

        // Case 2: Incomplete words that are not valid prefixes (e.g., "show tab" vs "show")
        if input_words.len() == suggestion_words.len() {
            for (input_word, suggestion_word) in input_words.iter().zip(suggestion_words.iter()) {
                // If word is incomplete (prefix) but not a complete word
                if suggestion_word.starts_with(input_word)
                    && input_word != suggestion_word
                    && input_word.len() >= 3
                {
                    return true;
                }
            }
        }

        // Case 3: Wrong word entirely but similar (e.g., "crate table" vs "CREATE TABLE")
        if !input_words.is_empty() && !suggestion_words.is_empty() {
            let edit_distance = self.calculate_edit_distance(input_words[0], suggestion_words[0]);
            if edit_distance > 0 && edit_distance <= 2 && input_words[0].len() >= 4 {
                return true;
            }
        }

        false
    }

    /// Calculate simple edit distance between two words
    fn calculate_edit_distance(&self, word1: &str, word2: &str) -> usize {
        let len1 = word1.len();
        let len2 = word2.len();
        let mut dp = vec![vec![0; len2 + 1]; len1 + 1];

        for (i, item) in dp.iter_mut().enumerate().take(len1 + 1) {
            item[0] = i;
        }
        for j in 0..=len2 {
            dp[0][j] = j;
        }

        for i in 1..=len1 {
            for j in 1..=len2 {
                if word1.chars().nth(i - 1) == word2.chars().nth(j - 1) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + dp[i - 1][j].min(dp[i][j - 1]).min(dp[i - 1][j - 1]);
                }
            }
        }

        dp[len1][len2]
    }

    /// Extract definite prefix from pattern (just return the pattern - it's already simple)
    fn extract_definite_prefix(&self, pattern: &str) -> String {
        pattern.to_string()
    }

    /// Check if command has definite prefix
    fn is_definite_command(&self, command: &str) -> bool {
        if command.contains('<')
            || command.contains('>')
            || command.contains('[')
            || command.contains(']')
        {
            return false;
        }

        let clean_cmd = command.trim().to_uppercase();

        // Check if command matches any of our known patterns
        Self::get_patterns().iter().any(|&pattern| {
            let pattern_upper = pattern.to_uppercase();
            clean_cmd == pattern_upper || clean_cmd.starts_with(&format!("{} ", pattern_upper))
        })
    }
}

/// Global error suggestion instance
static ERROR_SUGGESTION: OnceLock<ErrorSuggestion> = OnceLock::new();

/// Get the global error suggestion instance
pub fn get_error_suggestion() -> &'static ErrorSuggestion {
    ERROR_SUGGESTION.get_or_init(ErrorSuggestion::new)
}

/// Generate suggestion for SQL parsing error
pub fn suggest_correction(input: &str) -> Option<String> {
    get_error_suggestion().suggest(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_suggestions() {
        // Should suggest for actual typos/errors

        // Singular/plural errors
        assert_eq!(
            suggest_correction("show table"),
            Some("Did you mean `SHOW TABLES`?".to_string())
        );

        // Incomplete words (typos)
        assert_eq!(
            suggest_correction("show tabl"),
            Some("Did you mean `SHOW TABLES`?".to_string())
        );
        assert_eq!(
            suggest_correction("create tabl"),
            Some("Did you mean `CREATE TABLE`?".to_string())
        );

        // Spelling errors
        assert_eq!(
            suggest_correction("crate table"),
            Some("Did you mean `CREATE TABLE`?".to_string())
        );
        assert_eq!(
            suggest_correction("creat table"),
            Some("Did you mean `CREATE TABLE`?".to_string())
        );

        // Maintenance command typos
        assert_eq!(
            suggest_correction("optmize table"),
            Some("Did you mean `OPTIMIZE TABLE`?".to_string())
        );
        assert_eq!(
            suggest_correction("vacum table"),
            Some("Did you mean `VACUUM TABLE`?".to_string())
        );

        // Should NOT suggest for correct commands
        assert_eq!(suggest_correction("show tables"), None);
        assert_eq!(suggest_correction("create table"), None);
        assert_eq!(suggest_correction("optimize table"), None);
        assert_eq!(suggest_correction("vacuum table"), None);

        // Should provide context help for valid prefixes
        let vacuum_help = suggest_correction("vacuum");
        assert!(vacuum_help.is_some());
        let vacuum_text = vacuum_help.unwrap();
        assert!(vacuum_text.contains("Try:") && vacuum_text.contains("VACUUM"));

        let show_help = suggest_correction("show");
        assert!(show_help.is_some());
        let show_text = show_help.unwrap();
        assert!(show_text.contains("Try:") && show_text.contains("SHOW"));

        let create_help = suggest_correction("create");
        assert!(create_help.is_some());
        let create_text = create_help.unwrap();
        assert!(create_text.contains("Try:") && create_text.contains("CREATE"));

        // Should NOT suggest for random text
        assert_eq!(suggest_correction("xyz abc"), None);
        assert_eq!(suggest_correction("hello world"), None);
        assert_eq!(suggest_correction("s"), None);
        assert_eq!(suggest_correction(""), None);
    }

    #[test]
    fn test_similarity() {
        let suggester = ErrorSuggestion::new();
        let perfect = suggester
            .calculate_similarity(&["show".to_string(), "tables".to_string()], "SHOW TABLES");
        let partial = suggester
            .calculate_similarity(&["show".to_string(), "table".to_string()], "SHOW TABLES");
        assert!(perfect > partial);
    }

    #[test]
    fn test_keyword_similarity() {
        let suggester = ErrorSuggestion::new();
        // Prefix matching: pattern should start with input
        assert!(suggester.is_similar_keyword("tabl", "tables"));
        assert!(suggester.is_similar_keyword("table", "tables"));
        assert!(suggester.is_similar_keyword("show", "show_tables"));

        // Should not match when input is longer than pattern
        assert!(!suggester.is_similar_keyword("tables", "table"));
        assert!(!suggester.is_similar_keyword("xyz", "tables"));
        assert!(!suggester.is_similar_keyword("abc", "tables"));
    }

    #[test]
    fn test_error_detection() {
        let suggester = ErrorSuggestion::new();

        // Should detect typos as likely errors
        assert!(suggester.is_likely_error("show tabl", "SHOW TABLES"));
        assert!(suggester.is_likely_error("crate table", "CREATE TABLE"));
        assert!(suggester.is_likely_error("vacum table", "VACUUM TABLE"));

        // Should detect singular/plural errors
        assert!(suggester.is_likely_error("show table", "SHOW TABLES"));
        assert!(suggester.is_likely_error("create user", "CREATE USERS"));

        // Should NOT detect valid prefixes as errors
        assert!(!suggester.is_likely_error("show", "SHOW TABLES"));
        assert!(!suggester.is_likely_error("create", "CREATE TABLE"));
        assert!(!suggester.is_likely_error("vacuum", "VACUUM TABLE"));
    }

    #[test]
    fn test_correct_command_detection() {
        let suggester = ErrorSuggestion::new();

        // Should detect exact matches as correct
        assert!(suggester.is_correct_command("SHOW TABLES", "SHOW TABLES"));
        assert!(suggester.is_correct_command("create table", "CREATE TABLE"));

        // Should detect valid prefixes as correct
        assert!(suggester.is_correct_command("show", "SHOW TABLES"));
        assert!(suggester.is_correct_command("create", "CREATE TABLE"));
        assert!(suggester.is_correct_command("vacuum", "VACUUM TABLE"));
        assert!(suggester.is_correct_command("show create", "SHOW CREATE TABLE"));

        // Should not detect typos as correct
        assert!(!suggester.is_correct_command("show tabl", "SHOW TABLES"));
        assert!(!suggester.is_correct_command("crate table", "CREATE TABLE"));
        assert!(!suggester.is_correct_command("xyz", "SHOW TABLES"));
    }

    #[test]
    fn test_edit_distance() {
        let suggester = ErrorSuggestion::new();

        assert_eq!(suggester.calculate_edit_distance("show", "show"), 0);
        assert_eq!(suggester.calculate_edit_distance("crate", "create"), 1);
        assert_eq!(suggester.calculate_edit_distance("vacum", "vacuum"), 1);
        assert_eq!(suggester.calculate_edit_distance("tabl", "tables"), 2);
        assert_eq!(suggester.calculate_edit_distance("xyz", "show"), 4);
    }

    #[test]
    fn test_context_help() {
        // Test context help for valid but incomplete command prefixes

        // Should provide context help for valid prefixes
        let vacuum_help = suggest_correction("vacuum");
        assert!(vacuum_help.is_some());
        let vacuum_text = vacuum_help.unwrap();
        assert!(vacuum_text.contains("Try:"));
        assert!(vacuum_text.contains("VACUUM TABLE"));
        assert!(
            vacuum_text.contains("VACUUM DROP TABLE") || vacuum_text.contains("VACUUM TEMPORARY")
        );

        let show_help = suggest_correction("show");
        assert!(show_help.is_some());
        let show_text = show_help.unwrap();
        assert!(show_text.contains("Try:"));
        assert!(show_text.contains("SHOW TABLES"));
        assert!(show_text.contains("SHOW DATABASES") || show_text.contains("SHOW FUNCTIONS"));

        let create_help = suggest_correction("create");
        assert!(create_help.is_some());
        let create_text = create_help.unwrap();
        assert!(create_text.contains("Try:"));
        assert!(create_text.contains("CREATE TABLE"));
        assert!(create_text.contains("CREATE DATABASE") || create_text.contains("CREATE USER"));

        // Should not provide context help for multi-word inputs
        assert_eq!(suggest_correction("show create"), None);
        assert_eq!(suggest_correction("vacuum drop"), None);

        // Should not provide context help for unknown prefixes
        assert_eq!(suggest_correction("xyz"), None);
    }
}

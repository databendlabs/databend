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

const PATTERNS: &[&str] = &[
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
    "SHOW CATALOGS",
    "SHOW USER FUNCTIONS",
    "SHOW TABLE FUNCTIONS",
    "SHOW INDEXES",
    "SHOW STATISTICS",
    "SHOW WORKLOAD GROUPS",
    "SHOW ONLINE NODES",
    "VACUUM DROP TABLE",
    "VACUUM TEMPORARY FILES",
    "VACUUM TEMPORARY TABLES",
];

/// Generate suggestion for SQL parsing error
pub fn suggest_correction(input: &str) -> Option<String> {
    let input = input.trim();
    if input.len() < 2 {
        return None;
    }

    // Try context help first (for valid single-word prefixes)
    if let Some(help) = context_help(input) {
        return Some(help);
    }

    // Try error correction
    error_correction(input)
}

fn context_help(input: &str) -> Option<String> {
    let words: Vec<&str> = input.split_whitespace().collect();
    if words.len() != 1 {
        return None;
    }

    let prefix = words[0].to_uppercase();
    let matches: Vec<&str> = PATTERNS
        .iter()
        .filter(|p| p.split_whitespace().next().unwrap() == prefix)
        .take(3)
        .copied()
        .collect();

    match matches.len() {
        2 => Some(format!("Try: `{}` or `{}`", matches[0], matches[1])),
        3 => Some(format!(
            "Try: `{}`, `{}`, or `{}`",
            matches[0], matches[1], matches[2]
        )),
        _ => None,
    }
}

fn error_correction(input: &str) -> Option<String> {
    let input_tokens: Vec<&str> = input.split_whitespace().collect();
    let mut candidates: Vec<(&str, f64)> = Vec::new();

    for &pattern in PATTERNS {
        let score = similarity(&input_tokens, pattern);
        if score >= 4.0 && !is_correct(input, pattern) && is_error(input, pattern) {
            candidates.push((pattern, score));
        }
    }

    if candidates.is_empty() {
        return None;
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    match candidates.len() {
        1 => Some(format!("Did you mean `{}`?", candidates[0].0)),
        2 => Some(format!(
            "Did you mean `{}` or `{}`?",
            candidates[0].0, candidates[1].0
        )),
        _ => Some(format!(
            "Did you mean `{}`, `{}`, or `{}`?",
            candidates[0].0, candidates[1].0, candidates[2].0
        )),
    }
}

fn similarity(input_tokens: &[&str], pattern: &str) -> f64 {
    let pattern_tokens: Vec<&str> = pattern.split_whitespace().collect();
    let mut score = 0.0;
    let mut matches = 0;

    for (i, &input_token) in input_tokens.iter().enumerate() {
        if i >= pattern_tokens.len() {
            break;
        }
        let pattern_token = pattern_tokens[i];

        if input_token.eq_ignore_ascii_case(pattern_token) {
            score += 2.0;
            matches += 1;
        } else if is_prefix(input_token, pattern_token) {
            score += 1.5;
            matches += 1;
        } else if is_typo(input_token, pattern_token) {
            score += 1.0;
            matches += 1;
        } else {
            break;
        }
    }

    if matches == input_tokens.len() && matches > 0 {
        score += 3.0;
    }
    if input_tokens.len() == 1 && matches == 1 {
        score += 2.0;
    }
    score
}

fn is_prefix(input: &str, pattern: &str) -> bool {
    pattern.to_lowercase().starts_with(&input.to_lowercase())
}

fn is_typo(input: &str, pattern: &str) -> bool {
    if input.len() < 3 {
        return false;
    }
    edit_distance(input, pattern) <= 2
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a = a.to_lowercase();
    let b = b.to_lowercase();
    if a.len().abs_diff(b.len()) > 2 {
        return 3;
    }

    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0; b.len() + 1];

    for (i, ch_a) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, ch_b) in b.chars().enumerate() {
            curr[j + 1] = if ch_a == ch_b {
                prev[j]
            } else {
                1 + prev[j].min(prev[j + 1]).min(curr[j])
            };
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

fn is_correct(input: &str, _pattern: &str) -> bool {
    let input = input.trim().to_uppercase();
    PATTERNS.iter().any(|&p| p == input)
}

fn is_error(input: &str, suggestion: &str) -> bool {
    let input_upper = input.to_uppercase();
    let input_words: Vec<&str> = input_upper.split_whitespace().collect();
    let sugg_words: Vec<&str> = suggestion.split_whitespace().collect();

    if input_words.len() == sugg_words.len() {
        // Same word count: check for typos or prefix matches
        input_words
            .iter()
            .zip(sugg_words.iter())
            .any(|(i, s)| i != s && (is_typo(i, s) || is_prefix(i, s)))
    } else if input_words.len() < sugg_words.len() {
        // Input is shorter: check if it's a valid prefix with potential typo in last word
        if input_words.is_empty() {
            return false;
        }

        // All but last word must match exactly
        for i in 0..input_words.len() - 1 {
            if input_words[i] != sugg_words[i] {
                return false;
            }
        }

        // Last word can be typo or prefix
        let last_input = input_words[input_words.len() - 1];
        let last_sugg = sugg_words[input_words.len() - 1];
        last_input != last_sugg
            && (is_typo(last_input, last_sugg) || is_prefix(last_input, last_sugg))
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_suggestions() {
        // Typo suggestions (may be single or multiple)
        let show_tabl = suggest_correction("show tabl").unwrap();
        assert!(show_tabl.contains("SHOW TABLES"));
        assert!(show_tabl.contains("Did you mean"));

        // Test vacuum suggestions
        let vacum_drop_result = suggest_correction("vacum drop table");
        if let Some(suggestion) = vacum_drop_result {
            assert!(suggestion.contains("VACUUM DROP TABLE"));
        }

        let vacum_temp_result = suggest_correction("vacum temporary files");
        if let Some(suggestion) = vacum_temp_result {
            assert!(suggestion.contains("VACUUM TEMPORARY FILES"));
        }

        // Multiple suggestions for ambiguous cases
        let table_suggestion = suggest_correction("show table").unwrap();
        assert!(table_suggestion.contains("SHOW TABLES"));
        assert!(table_suggestion.contains("Did you mean"));
        // May contain multiple suggestions
        if table_suggestion.contains(" or ") {
            assert!(table_suggestion.contains("SHOW TABLE FUNCTIONS"));
        }

        // Correct commands
        assert_eq!(suggest_correction("show tables"), None);
        assert_eq!(suggest_correction("vacuum drop table"), None);

        // Context help
        let vacuum_help = suggest_correction("vacuum").unwrap();
        assert!(vacuum_help.contains("Try:") && vacuum_help.contains("VACUUM"));

        let show_help = suggest_correction("show").unwrap();
        assert!(show_help.contains("Try:") && show_help.contains("SHOW"));

        // Invalid
        assert_eq!(suggest_correction("xyz abc"), None);
        assert_eq!(suggest_correction("s"), None);
    }

    #[test]
    fn test_similarity() {
        assert!(
            similarity(&["show", "tables"], "SHOW TABLES")
                > similarity(&["show", "table"], "SHOW TABLES")
        );
    }

    #[test]
    fn test_keyword_similarity() {
        assert!(is_prefix("tabl", "tables"));
        assert!(!is_prefix("tables", "table"));
    }

    #[test]
    fn test_error_detection() {
        assert!(is_error("show tabl", "SHOW TABLES"));
        assert!(is_error("crate table", "CREATE TABLE"));
        assert!(!is_error("show", "SHOW TABLES"));
    }

    #[test]
    fn test_correct_command_detection() {
        assert!(!is_correct("show", "SHOW TABLES"));
        assert!(is_correct("SHOW TABLES", "SHOW TABLES"));
        assert!(!is_correct("show tabl", "SHOW TABLES"));
    }

    #[test]
    fn test_edit_distance() {
        assert_eq!(edit_distance("show", "show"), 0);
        assert_eq!(edit_distance("crate", "create"), 1);
        assert_eq!(edit_distance("tabl", "tables"), 2);
    }

    #[test]
    fn test_multiple_suggestions() {
        // Test "show table" - should suggest the best match
        let show_table = suggest_correction("show table").unwrap();
        println!("show table suggestion: {}", show_table);
        assert!(show_table.contains("SHOW TABLES"));
        assert!(show_table.starts_with("Did you mean"));

        // Test case that could potentially have multiple high-scoring matches
        let vacuum_result = suggest_correction("vacuum temp");
        if let Some(vacuum_suggestion) = vacuum_result {
            println!("vacuum temp suggestion: {}", vacuum_suggestion);
            assert!(vacuum_suggestion.contains("VACUUM TEMPORARY"));
            // Could be "VACUUM TEMPORARY FILES" or "VACUUM TEMPORARY TABLES"
            assert!(
                vacuum_suggestion.contains("FILES")
                    || vacuum_suggestion.contains("TABLES")
                    || vacuum_suggestion.contains(" or ")
            );
        }
    }

    #[test]
    fn test_context_help() {
        // Should provide context help for valid prefixes
        let vacuum_help = suggest_correction("vacuum").unwrap();
        assert!(vacuum_help.contains("Try:"));
        assert!(
            vacuum_help.contains("VACUUM DROP TABLE") && vacuum_help.contains("VACUUM TEMPORARY")
        );

        let show_help = suggest_correction("show").unwrap();
        assert!(show_help.contains("Try:"));
        assert!(show_help.contains("SHOW TABLES"));
        assert!(show_help.contains("SHOW DATABASES") || show_help.contains("SHOW FUNCTIONS"));

        // Should not provide context help for multi-word inputs or unknown prefixes
        assert_eq!(suggest_correction("show create"), None);
        assert_eq!(suggest_correction("vacuum drop"), None);
        assert_eq!(suggest_correction("xyz"), None);
    }
}

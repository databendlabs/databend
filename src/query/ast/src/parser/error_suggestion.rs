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
    if input.len() < 2 || input.len() > 128 {
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
    let input_upper = input.trim().to_uppercase();

    // If input exactly matches a pattern, no correction needed
    if PATTERNS.iter().any(|&p| p == input_upper) {
        return None;
    }

    // Only suggest for inputs that look like SQL commands
    if input_tokens.is_empty() {
        return None;
    }

    let first_token = input_tokens[0].to_uppercase();

    // Extract valid starts from PATTERNS
    let mut valid_starts: Vec<&str> = PATTERNS
        .iter()
        .map(|pattern| pattern.split_whitespace().next().unwrap())
        .collect();
    valid_starts.sort();
    valid_starts.dedup();

    // Check for exact match or typo in first token
    let is_sql_command = valid_starts.contains(&first_token.as_str())
        || valid_starts.iter().any(|&keyword| {
            let distance = edit_distance(&first_token, keyword);
            distance <= 2 && distance < keyword.len() / 2
        });

    if !is_sql_command {
        return None;
    }

    let mut candidates: Vec<(&str, f64)> = Vec::new();

    for &pattern in PATTERNS {
        let score = calculate_similarity(&input_tokens, pattern);
        if score > 0.0 {
            candidates.push((pattern, score));
        }
    }

    if candidates.is_empty() {
        return None;
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // Only suggest if best match has high similarity and is actually relevant
    let best_score = candidates[0].1;
    if best_score < 4.0 {
        return None;
    }

    // Include candidates that are truly relevant
    // Use a reasonable threshold - suggest if score is within 1.0 of best
    let threshold = best_score - 1.0;
    let good_candidates: Vec<&str> = candidates
        .iter()
        .take_while(|(_, score)| *score >= threshold)
        .take(3)
        .map(|(pattern, _)| *pattern)
        .collect();

    match good_candidates.len() {
        1 => Some(format!("Did you mean `{}`?", good_candidates[0])),
        2 => Some(format!(
            "Did you mean `{}` or `{}`?",
            good_candidates[0], good_candidates[1]
        )),
        _ => Some(format!(
            "Did you mean `{}`, `{}`, or `{}`?",
            good_candidates[0], good_candidates[1], good_candidates[2]
        )),
    }
}

fn calculate_similarity(input_tokens: &[&str], pattern: &str) -> f64 {
    let pattern_tokens: Vec<&str> = pattern.split_whitespace().collect();
    let mut total_score = 0.0;
    let mut matched_tokens = 0;

    // Compare each input token with corresponding pattern token
    for (i, &input_token) in input_tokens.iter().enumerate() {
        if i >= pattern_tokens.len() {
            break;
        }
        let pattern_token = pattern_tokens[i];

        if input_token.eq_ignore_ascii_case(pattern_token) {
            // Exact match
            total_score += 3.0;
            matched_tokens += 1;
        } else if pattern_token
            .to_lowercase()
            .starts_with(&input_token.to_lowercase())
        {
            // Prefix match - give good score but always less than exact match (3.0)
            let input_len = input_token.len() as f64;
            let pattern_len = pattern_token.len() as f64;
            let prefix_ratio = input_len / pattern_len;

            // Only give good scores for meaningful prefixes (at least 3 chars or >50% of word)
            if input_len >= 3.0 || prefix_ratio > 0.5 {
                total_score += 1.5 + prefix_ratio * 1.0; // Max 2.5, always < 3.0 exact match
            } else {
                total_score += 0.5; // Very low score for short prefixes like "t"
            }
            matched_tokens += 1;
        } else {
            // Typo match
            let distance = edit_distance(input_token, pattern_token);
            let max_distance = if pattern_token.len() > 6 { 3 } else { 2 };
            if distance <= max_distance && distance < pattern_token.len() / 2 {
                total_score += 2.0 - (distance as f64 * 0.5);
                matched_tokens += 1;
            }
        }
    }

    // Bonus for matching more tokens
    if matched_tokens == input_tokens.len() && matched_tokens > 0 {
        total_score += matched_tokens as f64;
    }

    total_score
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typo_corrections() {
        // Typo corrections - may return multiple suggestions if scores are close
        assert_eq!(
            suggest_correction("show tabl"), // typos:disable-line
            Some("Did you mean `SHOW TABLE FUNCTIONS` or `SHOW TABLES`?".to_string())
        );
        assert_eq!(
            suggest_correction("vacum drop table"), // typos:disable-line
            Some("Did you mean `VACUUM DROP TABLE`?".to_string())
        );
        assert_eq!(
            suggest_correction("vacuum tempare files"),
            Some("Did you mean `VACUUM TEMPORARY FILES`?".to_string())
        );
    }

    #[test]
    fn test_multiple_suggestions() {
        // Should suggest both relevant options for ambiguous input
        assert_eq!(
            suggest_correction("show table"),
            Some("Did you mean `SHOW TABLE FUNCTIONS` or `SHOW TABLES`?".to_string())
        );

        // Multiple suggestions when scores are very close
        assert_eq!(
            suggest_correction("vacuum temp"),
            Some("Did you mean `VACUUM TEMPORARY FILES` or `VACUUM TEMPORARY TABLES`?".to_string())
        );
    }

    #[test]
    fn test_context_help() {
        // Single word prefixes should get context help
        assert_eq!(
            suggest_correction("vacuum"),
            Some(
                "Try: `VACUUM DROP TABLE`, `VACUUM TEMPORARY FILES`, or `VACUUM TEMPORARY TABLES`"
                    .to_string()
            )
        );

        let result = suggest_correction("show").unwrap();
        assert!(result.starts_with("Try: "));
        assert!(result.contains("SHOW TABLES"));
    }

    #[test]
    fn test_exact_matches() {
        // Exact matches should return None
        assert_eq!(suggest_correction("show tables"), None);
        assert_eq!(suggest_correction("VACUUM DROP TABLE"), None);
        assert_eq!(suggest_correction("Show Tables"), None);
    }

    #[test]
    fn test_no_suggestions() {
        // Should not suggest for unrelated input
        assert_eq!(suggest_correction("xyz abc def"), None);
        assert_eq!(suggest_correction("create index"), None);
        assert_eq!(suggest_correction("s"), None);
        assert_eq!(suggest_correction(""), None);
    }

    #[test]
    fn test_similarity_scoring() {
        // Exact match should score higher than partial
        assert!(
            calculate_similarity(&["show", "tables"], "SHOW TABLES")
                > calculate_similarity(&["show", "table"], "SHOW TABLES")
        );
    }

    #[test]
    fn test_edit_distance() {
        assert_eq!(edit_distance("show", "show"), 0);
        assert_eq!(edit_distance("tempare", "temporary"), 3);
        assert_eq!(edit_distance("tabl", "tables"), 2); // typos:disable-line
    }

    #[test]
    fn test_performance_limits() {
        // Should return None for very short inputs
        assert_eq!(suggest_correction("a"), None);
        assert_eq!(suggest_correction(""), None);

        // Should return None for inputs longer than 128 characters
        let long_sql = "a".repeat(129);
        assert_eq!(suggest_correction(&long_sql), None);

        // Should work for reasonable length inputs
        assert!(suggest_correction("show table").is_some());
    }

    #[test]
    fn test_prefix_matching_accuracy() {
        // "show tab" should suggest table-related commands, not random ones
        let result = suggest_correction("show tab").unwrap();
        // Should prioritize table-related suggestions
        assert_eq!(
            result,
            "Did you mean `SHOW TABLE FUNCTIONS` or `SHOW TABLES`?".to_string()
        );
    }

    #[test]
    fn test_valid_starts_extraction() {
        // Test that valid starts are correctly extracted from PATTERNS
        let mut expected_starts: Vec<&str> = PATTERNS
            .iter()
            .map(|pattern| pattern.split_whitespace().next().unwrap())
            .collect();
        expected_starts.sort();
        expected_starts.dedup();

        // Should contain SHOW and VACUUM from our patterns
        assert!(expected_starts.contains(&"SHOW"));
        assert!(expected_starts.contains(&"VACUUM"));

        // Should recognize valid starts with similar commands
        assert_eq!(
            suggest_correction("show table"),
            Some("Did you mean `SHOW TABLE FUNCTIONS` or `SHOW TABLES`?".to_string())
        );
        assert_eq!(
            suggest_correction("vacuum temp"),
            Some("Did you mean `VACUUM TEMPORARY FILES` or `VACUUM TEMPORARY TABLES`?".to_string())
        );

        // Should not recognize invalid starts
        assert_eq!(suggest_correction("create unknown"), None);
        assert_eq!(suggest_correction("select unknown"), None);
    }
}

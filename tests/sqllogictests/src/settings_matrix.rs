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

//! Per-file settings matrix.
//!
//! A test file can ask to be run once more with non-default session settings
//! by adding a directive comment:
//!
//! ```text
//! # run-with-settings: enable_fixed_rows_sort=0
//! # run-with-settings: enable_experimental_new_join=0, join_spilling_memory_ratio=0
//! ```
//!
//! Every directive line adds one extra pass over the whole file. Each pass
//! applies its `SET` statements to every new connection right after the
//! sandbox is initialised and before the first record runs, so a `SET`
//! inside the file still wins over the directive. This gives a setting-gated
//! execution path (or its legacy fallback) the same coverage as the default
//! path without duplicating the test file.

use std::fmt;
use std::path::Path;

use crate::error::DSqlLogicTestError;
use crate::error::Result;

const DIRECTIVE: &str = "run-with-settings:";

/// One group of session settings applied to every connection of an extra
/// test pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsGroup(Vec<(String, String)>);

impl SettingsGroup {
    /// Render each entry as a `SET` statement. Digit-only values are emitted
    /// as numbers, everything else is single-quoted.
    pub fn set_statements(&self) -> Vec<String> {
        self.0
            .iter()
            .map(|(key, value)| {
                if value.chars().all(|c| c.is_ascii_digit()) {
                    format!("SET {key} = {value}")
                } else {
                    format!("SET {key} = '{}'", value.replace('\'', "''"))
                }
            })
            .collect()
    }
}

impl fmt::Display for SettingsGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered: Vec<String> = self.0.iter().map(|(k, v)| format!("{k}={v}")).collect();
        write!(f, "{}", rendered.join(","))
    }
}

fn parse_settings_group(input: &str) -> std::result::Result<SettingsGroup, String> {
    let mut entries = Vec::new();
    for raw in input.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let Some((key, value)) = raw.split_once('=') else {
            return Err(format!("invalid setting '{raw}', expected key=value"));
        };
        let (key, value) = (key.trim(), value.trim());
        if key.is_empty() || value.is_empty() {
            return Err(format!("invalid setting '{raw}', expected key=value"));
        }
        entries.push((key.to_string(), value.to_string()));
    }
    if entries.is_empty() {
        return Err("directive must contain at least one key=value".to_string());
    }
    Ok(SettingsGroup(entries))
}

/// Extract every `# run-with-settings:` directive from the file content, in
/// file order. A malformed directive is a hard error so that a typo cannot
/// silently drop a pass.
pub fn parse_settings_directives(content: &str) -> std::result::Result<Vec<SettingsGroup>, String> {
    let mut groups = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let Some(comment) = line.trim_start().strip_prefix('#') else {
            continue;
        };
        let comment = comment.trim_start();
        if comment.len() < DIRECTIVE.len()
            || !comment[..DIRECTIVE.len()].eq_ignore_ascii_case(DIRECTIVE)
        {
            continue;
        }
        let group = parse_settings_group(&comment[DIRECTIVE.len()..])
            .map_err(|e| format!("line {}: {e}", idx + 1))?;
        groups.push(group);
    }
    Ok(groups)
}

/// Read the file and return its extra settings passes.
pub fn collect_settings_passes(path: &Path) -> Result<Vec<SettingsGroup>> {
    let content = std::fs::read_to_string(path)?;
    parse_settings_directives(&content).map_err(|e| {
        DSqlLogicTestError::SelfError(format!(
            "invalid `# {DIRECTIVE}` directive in {}: {e}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_directives_in_order() {
        let content = "\
# run-with-settings: enable_fixed_rows_sort=0
statement ok
select 1

#   Run-With-Settings:  enable_experimental_new_join = 0 , sql_dialect=PostgreSQL,
# unrelated comment: a=b
";
        let groups = parse_settings_directives(content).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].set_statements(), vec![
            "SET enable_fixed_rows_sort = 0".to_string()
        ]);
        assert_eq!(groups[1].set_statements(), vec![
            "SET enable_experimental_new_join = 0".to_string(),
            "SET sql_dialect = 'PostgreSQL'".to_string(),
        ]);
        assert_eq!(
            groups[1].to_string(),
            "enable_experimental_new_join=0,sql_dialect=PostgreSQL"
        );
    }

    #[test]
    fn no_directive_means_no_extra_pass() {
        assert!(
            parse_settings_directives("statement ok\nselect 1\n")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejects_malformed_directives() {
        for content in [
            "# run-with-settings:",
            "# run-with-settings: no_equal_sign",
            "# run-with-settings: =1",
            "# run-with-settings: key=",
        ] {
            let err = parse_settings_directives(content).unwrap_err();
            assert!(err.starts_with("line 1:"), "{err}");
        }
    }

    #[test]
    fn quotes_string_values() {
        let group = parse_settings_group("timezone=Asia/Shanghai,x=it's").unwrap();
        assert_eq!(group.set_statements(), vec![
            "SET timezone = 'Asia/Shanghai'".to_string(),
            "SET x = 'it''s'".to_string(),
        ]);
    }
}

// Copyright 2021 Datafuse Labs.
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

use comfy_table::Cell;
use comfy_table::Table;
use common_exception::Result;
use regex::bytes::Regex;

use crate::DataBlock;

///! Create a visual representation of record batches
pub fn pretty_format_blocks(results: &[DataBlock]) -> Result<String> {
    Ok(create_table(results)?.trim_fmt())
}

pub fn assert_blocks_eq(expect: Vec<&str>, blocks: &[DataBlock]) {
    assert_blocks_eq_with_name("", expect, blocks)
}

/// Assert with order sensitive.
/// ['a', 'b'] not equals ['b', 'a']
pub fn assert_blocks_eq_with_name(test_name: &str, expect: Vec<&str>, blocks: &[DataBlock]) {
    let expected_lines: Vec<String> = expect.iter().map(|&s| s.into()).collect();
    let formatted = pretty_format_blocks(blocks).unwrap();
    let actual_lines: Vec<&str> = formatted.trim().lines().collect();

    assert_eq!(
        expected_lines, actual_lines,
        "{:#?}\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        test_name, expected_lines, actual_lines
    );
}

/// Sorted assert.
pub fn assert_blocks_sorted_eq(expect: Vec<&str>, blocks: &[DataBlock]) {
    assert_blocks_sorted_eq_with_name("", expect, blocks)
}

/// Assert with order insensitive.
/// ['a', 'b'] equals ['b', 'a']
pub fn assert_blocks_sorted_eq_with_name(test_name: &str, expect: Vec<&str>, blocks: &[DataBlock]) {
    let mut expected_lines: Vec<String> = expect.iter().map(|&s| s.into()).collect();

    // sort except for header + footer
    let num_lines = expected_lines.len();
    if num_lines > 3 {
        expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    let formatted = pretty_format_blocks(blocks).unwrap();
    let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

    // sort except for header + footer
    let num_lines = actual_lines.len();
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    assert_eq!(
        expected_lines, actual_lines,
        "{:#?}\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        test_name, expected_lines, actual_lines
    );
}

///! Convert a column of record batches into a table
fn create_table(results: &[DataBlock]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                let str = format!("{}", column.get_checked(row)?);
                cells.push(Cell::new(&str));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

pub fn assert_blocks_sorted_eq_with_regex(patterns: Vec<&str>, blocks: &[DataBlock]) {
    let mut re_patterns: Vec<String> = patterns
        .iter()
        .map(|&s| {
            let mut re_pattern: String = "^".into();
            re_pattern += s;
            re_pattern += "$";
            re_pattern
        })
        .collect();

    // sort except for header + footer
    let num_lines = re_patterns.len();
    if num_lines > 3 {
        re_patterns.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    let formatted = pretty_format_blocks(blocks).unwrap();
    let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

    assert_eq!(
        num_lines,
        actual_lines.len(),
        "expected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        re_patterns,
        actual_lines,
    );

    // sort except for header + footer
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    for i in 0..num_lines {
        let re = Regex::new(&re_patterns[i]).unwrap();
        if !re.is_match(actual_lines[i].as_bytes()) {
            panic!(
                "expected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                re_patterns, actual_lines
            )
        }
    }
}

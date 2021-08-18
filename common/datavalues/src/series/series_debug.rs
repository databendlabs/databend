// Copyright 2020 Datafuse Labs.
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

use super::Series;

///! Create a visual representation of record batches
pub fn pretty_format_series(results: &[Series]) -> Result<String> {
    Ok(create_table(results)?.trim_fmt())
}

pub fn assert_series_eq(expect: Vec<&str>, series: &[Series]) {
    assert_series_eq_with_name("", expect, series)
}

/// Assert with order sensitive.
/// ['a', 'b'] not equals ['b', 'a']
pub fn assert_series_eq_with_name(test_name: &str, expect: Vec<&str>, series: &[Series]) {
    let expected_lines: Vec<String> = expect.iter().map(|&s| s.into()).collect();
    let formatted = pretty_format_series(series).unwrap();
    let actual_lines: Vec<&str> = formatted.trim().lines().collect();

    assert_eq!(
        expected_lines, actual_lines,
        "{:#?}\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        test_name, expected_lines, actual_lines
    );
}

/// Sorted assert.
pub fn assert_series_sorted_eq(expect: Vec<&str>, series: &[Series]) {
    assert_series_sorted_eq_with_name("", expect, series)
}

/// Assert with order insensitive.
/// ['a', 'b'] equals ['b', 'a']
pub fn assert_series_sorted_eq_with_name(test_name: &str, expect: Vec<&str>, series: &[Series]) {
    let mut expected_lines: Vec<String> = expect.iter().map(|&s| s.into()).collect();

    // sort except for header + footer
    let num_lines = expected_lines.len();
    if num_lines > 3 {
        expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    let formatted = pretty_format_series(series).unwrap();
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

///! Convert a series of record batches into a table
fn create_table(results: &[Series]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let header = vec![Cell::new("series")];
    table.set_header(header);

    for series in results {
        for row in 0..series.len() {
            let mut cells = Vec::new();
            let str = format!("{}", series.try_get(row)?);
            cells.push(Cell::new(&str));
            table.add_row(cells);
        }
    }

    Ok(table)
}

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use prettytable::format;
use prettytable::Cell;
use prettytable::Row;
use prettytable::Table;

use super::Series;

///! Create a visual representation of record batches
pub fn pretty_format_series(results: &[Series]) -> Result<String> {
    Ok(create_table(results)?.to_string())
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
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    if results.is_empty() {
        return Ok(table);
    }

    let header = vec![Cell::new("series")];
    table.set_titles(Row::new(header));
    for series in results {
        for row in 0..series.len() {
            let mut cells = Vec::new();
            let str = format!("{}", series.try_get(row)?);
            cells.push(Cell::new(&str));
            table.add_row(Row::new(cells));
        }
    }

    Ok(table)
}

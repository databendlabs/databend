// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use prettytable::format;
use prettytable::Cell;
use prettytable::Row;
use prettytable::Table;

use crate::DataBlock;

///! Create a visual representation of record batches
pub fn pretty_format_blocks(results: &[DataBlock]) -> Result<String> {
    Ok(create_table(results)?.to_string())
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

///! Convert a series of record batches into a table
fn create_table(results: &[DataBlock]) -> Result<Table> {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_titles(Row::new(header));

    for batch in results {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let series = batch.column(col).to_array()?;
                let str = format!("{}", series.try_get(row)?);
                cells.push(Cell::new(&str));
            }
            table.add_row(Row::new(cells));
        }
    }

    Ok(table)
}

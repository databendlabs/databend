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

use comfy_table::Cell;
use comfy_table::CellAlignment;
use comfy_table::Table;
use common_exception::Result;

use crate::DataBlock;
use crate::DataSchemaRef;

/// ! Create a visual representation of record batches
pub fn pretty_format_blocks(results: &[DataBlock]) -> Result<String> {
    let block: DataBlock = DataBlock::concat(results)?;
    Ok(block.to_string())
}

pub fn assert_blocks_eq(expect: Vec<&str>, blocks: &[DataBlock]) {
    assert_blocks_eq_with_name("", expect, blocks)
}

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

pub fn box_render(schema: &DataSchemaRef, blocks: &[DataBlock]) -> Result<String> {
    let table = create_box_table(schema, blocks);
    Ok(table.to_string())
}

/// Convert a series of rows into a table
/// This format function is from duckdb's box_renderer:
/// https://github.com/duckdb/duckdb/blob/b475a57930f0a6c5163c82186e74b18391250ab0/src/common/box_renderer.cpp
fn create_box_table(schema: &DataSchemaRef, results: &[DataBlock]) -> Table {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return table;
    }

    let mut header = Vec::with_capacity(schema.fields().len());
    let mut aligns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let cell = Cell::new(format!("{}\n{}", field.name(), field.data_type(),))
            .set_alignment(CellAlignment::Center);

        header.push(cell);

        if field.data_type().remove_nullable().is_numeric() {
            aligns.push(CellAlignment::Right);
        } else {
            aligns.push(CellAlignment::Left);
        }
    }
    table.set_header(header);
    if results.is_empty() {
        return table;
    }

    const MAX_ROW: usize = 40;
    let row_count: usize = results.iter().map(|block| block.num_rows()).sum();
    let mut rows_to_render = row_count.min(MAX_ROW);

    if row_count <= MAX_ROW + 3 {
        // hiding rows adds 3 extra rows
        // so hiding rows makes no sense if we are only slightly over the limit
        // if we are 1 row over the limit hiding rows will actually increase the number of lines we display!
        // in this case render all the rows
        // 	rows_to_render = row_count;
        rows_to_render = row_count;
    }

    let (top_rows, bottom_rows) = if rows_to_render == row_count {
        (row_count, 0usize)
    } else {
        let top_rows = rows_to_render / 2 + (rows_to_render % 2 != 0) as usize;
        (top_rows, rows_to_render - top_rows)
    };

    if bottom_rows == 0 {
        for block in results {
            for row in 0..block.num_rows() {
                let mut cells = Vec::new();
                for (align, block_entry) in aligns.iter().zip(block.columns()) {
                    let cell =
                        Cell::new(block_entry.value.index(row).unwrap()).set_alignment(*align);
                    cells.push(cell);
                }
                table.add_row(cells);
            }
        }
        return table;
    }

    let top_collection = results.first().unwrap();
    let bottom_collection = results.last().unwrap();
    let top_rows = top_collection.num_rows().min(top_rows);
    for row in 0..top_rows {
        let mut cells: Vec<Cell> = Vec::new();
        for (align, block_entry) in aligns.iter().zip(top_collection.columns()) {
            let cell = Cell::new(block_entry.value.index(row).unwrap()).set_alignment(*align);
            cells.push(cell);
        }
        table.add_row(cells);
    }

    // render the bottom rows
    // first render the divider
    let mut cells: Vec<Cell> = Vec::new();
    for align in aligns.iter() {
        let cell = Cell::new("·").set_alignment(*align);
        cells.push(cell);
    }

    for _ in 0..3 {
        table.add_row(cells.clone());
    }

    let take_num = if bottom_collection.num_rows() > bottom_rows {
        bottom_collection.num_rows() - bottom_rows
    } else {
        0
    };

    for row in take_num..bottom_collection.num_rows() {
        let mut cells: Vec<Cell> = Vec::new();
        for (align, block_entry) in aligns.iter().zip(bottom_collection.columns()) {
            let cell = Cell::new(block_entry.value.index(row).unwrap()).set_alignment(*align);
            cells.push(cell);
        }
        table.add_row(cells);
    }

    let row_count_str = format!("{} rows", row_count);
    let show_count_str = format!("({} shown)", top_rows + bottom_rows);
    table.add_row(vec![Cell::new(row_count_str).set_alignment(aligns[0])]);
    table.add_row(vec![Cell::new(show_count_str).set_alignment(aligns[0])]);
    table
}

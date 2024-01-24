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

use std::collections::HashSet;

use comfy_table::Cell;
use comfy_table::CellAlignment;
use comfy_table::Table;
use databend_common_exception::Result;
use terminal_size::terminal_size;
use terminal_size::Width;
use unicode_segmentation::UnicodeSegmentation;

use crate::DataBlock;
use crate::DataSchemaRef;

/// ! Create a visual representation of record batches
pub fn pretty_format_blocks(results: &[DataBlock]) -> Result<String> {
    let block = DataBlock::concat(results)?;
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

pub fn assert_block_value_eq(a: &DataBlock, b: &DataBlock) {
    assert!(a.num_columns() == b.num_columns());
    assert!(a.num_rows() == b.num_rows());
    for i in 0..a.num_columns() {
        assert!(a.columns()[i].eq(&b.columns()[i]));
    }
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

/// Assert with order insensitive.
/// ['a', 'b'] equals ['b', 'a']
pub fn assert_two_blocks_sorted_eq_with_name(
    test_name: &str,
    expect: &[DataBlock],
    blocks: &[DataBlock],
) {
    let expected = pretty_format_blocks(expect).unwrap();
    let mut expected_lines: Vec<&str> = expected.trim().lines().collect();

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

pub fn box_render(
    schema: &DataSchemaRef,
    blocks: &[DataBlock],
    max_rows: usize,
    max_width: usize,
    max_col_width: usize,
    replace_newline: bool,
) -> Result<String> {
    let table = create_box_table(
        schema,
        blocks,
        max_rows,
        max_width,
        max_col_width,
        replace_newline,
    );
    Ok(table.to_string())
}

/// Convert a series of rows into a table
/// This format function is from duckdb's box_renderer:
/// https://github.com/duckdb/duckdb/blob/b475a57930f0a6c5163c82186e74b18391250ab0/src/common/box_renderer.cpp
fn create_box_table(
    schema: &DataSchemaRef,
    results: &[DataBlock],
    max_rows: usize,
    mut max_width: usize,
    max_col_width: usize,
    replace_newline: bool,
) -> Table {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return table;
    }

    let mut widths = vec![];
    let mut column_map = vec![];

    if max_width == 0 {
        let size = terminal_size();
        if let Some((Width(w), _)) = size {
            max_width = w as usize;
        }
    }

    let row_count: usize = results.iter().map(|block| block.num_rows()).sum();
    let mut rows_to_render = row_count.min(max_rows);

    if row_count <= max_rows + 3 {
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

    let mut res_vec: Vec<Vec<String>> = vec![];

    let mut rows = 0;
    for block in results {
        for row in 0..block.num_rows() {
            rows += 1;
            if rows > top_rows && rows <= row_count - bottom_rows {
                continue;
            }

            let mut v = vec![];
            for block_entry in block.columns() {
                let value = block_entry.value.index(row).unwrap().to_string();
                if replace_newline {
                    v.push(value.to_string().replace('\n', "\\n"));
                } else {
                    v.push(value.to_string());
                }
            }
            res_vec.push(v);
        }
    }

    // "..." take up three lengths
    if max_width > 0 {
        (widths, column_map) =
            compute_render_widths(schema, max_width, max_col_width + 3, &res_vec);
    }

    let mut header = Vec::with_capacity(schema.fields().len());
    let mut aligns = Vec::with_capacity(schema.fields().len());
    render_head(
        schema,
        &mut widths,
        &mut column_map,
        &mut header,
        &mut aligns,
    );
    table.set_header(header);

    if column_map.is_empty() {
        for values in res_vec.iter().take(top_rows) {
            let mut cells: Vec<Cell> = Vec::new();
            for (idx, align) in aligns.iter().enumerate() {
                let cell = Cell::new(&values[idx]).set_alignment(*align);
                cells.push(cell);
            }
            table.add_row(cells);
        }
    } else {
        for values in res_vec.iter().take(top_rows) {
            let mut cells: Vec<Cell> = Vec::new();
            for (idx, col_index) in column_map.iter().enumerate() {
                if *col_index == -1 {
                    let cell = Cell::new("...").set_alignment(CellAlignment::Center);
                    cells.push(cell);
                } else {
                    let mut value = values[*col_index as usize].clone();
                    if value.len() + 3 > widths[idx] {
                        let element_size = if widths[idx] >= 6 { widths[idx] - 6 } else { 0 };
                        value = String::from_utf8(
                            value
                                .graphemes(true)
                                .take(element_size)
                                .flat_map(|g| g.as_bytes().iter())
                                .copied() // copied converts &u8 into u8
                                .chain(b"...".iter().copied())
                                .collect::<Vec<u8>>(),
                        )
                        .unwrap();
                    }
                    let cell = Cell::new(value).set_alignment(aligns[idx]);
                    cells.push(cell);
                }
            }
            table.add_row(cells);
        }
    }

    // render the bottom rows
    // first render the divider
    if bottom_rows != 0 {
        let mut cells: Vec<Cell> = Vec::new();
        let display_res_len = res_vec.len();
        for align in aligns.iter() {
            let cell = Cell::new("·").set_alignment(*align);
            cells.push(cell);
        }

        for _ in 0..3 {
            table.add_row(cells.clone());
        }

        if column_map.is_empty() {
            for values in res_vec.iter().skip(top_rows) {
                let mut cells = Vec::new();
                for (idx, align) in aligns.iter().enumerate() {
                    let cell = Cell::new(&values[idx]).set_alignment(*align);
                    cells.push(cell);
                }
                table.add_row(cells);
            }
        } else {
            for values in res_vec.iter().skip(top_rows) {
                let mut cells: Vec<Cell> = Vec::new();
                for (idx, col_index) in column_map.iter().enumerate() {
                    if *col_index == -1 {
                        let cell = Cell::new("...").set_alignment(CellAlignment::Center);
                        cells.push(cell);
                    } else {
                        let mut value = values[*col_index as usize].clone();
                        if value.len() + 3 > widths[idx] {
                            let element_size = if widths[idx] >= 6 { widths[idx] - 6 } else { 0 };
                            value = String::from_utf8(
                                value
                                    .graphemes(true)
                                    .take(element_size)
                                    .flat_map(|g| g.as_bytes().iter())
                                    .copied() // copied converts &u8 into u8
                                    .chain(b"...".iter().copied())
                                    .collect::<Vec<u8>>(),
                            )
                            .unwrap();
                        }
                        let cell = Cell::new(value).set_alignment(aligns[idx]);
                        cells.push(cell);
                    }
                }
                table.add_row(cells);
            }
        }
        let row_count_str = format!("{} rows", row_count);
        let show_count_str = format!("({} shown)", display_res_len);
        table.add_row(vec![Cell::new(row_count_str).set_alignment(aligns[0])]);
        table.add_row(vec![Cell::new(show_count_str).set_alignment(aligns[0])]);
    }
    table
}

// compute render widths
fn compute_render_widths(
    schema: &DataSchemaRef,
    max_width: usize,
    max_col_width: usize,
    results: &Vec<Vec<String>>,
) -> (Vec<usize>, Vec<i32>) {
    let column_count = schema.fields().len();
    let mut widths = Vec::with_capacity(column_count);
    let mut total_length = 1;

    for field in schema.fields() {
        // head_name = field_name + "\n" + field_data_type
        let col_length = field.name().len().max(field.data_type().to_string().len());
        widths.push(col_length + 3);
    }

    for values in results {
        for (idx, value) in values.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len() + 3);
        }
    }

    for width in &widths {
        // each column has a space at the beginning, and a space plus a pipe (|) at the end
        // hence + 3
        total_length += width;
    }

    let mut pruned_columns = HashSet::new();
    if total_length > max_width {
        for w in &mut widths {
            if *w > max_col_width {
                let max_diff = *w - max_col_width;
                if total_length - max_diff <= max_width {
                    *w -= total_length - max_width;
                    total_length = max_width;
                    break;
                } else {
                    *w = max_col_width;
                    total_length -= max_diff;
                }
            }
        }
        if total_length > max_width {
            // the total length is still too large
            // we need to remove columns!
            // first, we add 6 characters to the total length
            // this is what we need to add the "..." in the middle
            total_length += 6;
            // now select columns to prune
            // we select columns in zig-zag order starting from the middle
            // e.g. if we have 10 columns, we remove #5, then #4, then #6, then #3, then #7, etc
            let mut offset: i32 = 0;
            while total_length > max_width {
                let c = column_count as i32 / 2 + offset;
                if c < 0 {
                    // c < 0 means no column can display
                    return ([3].to_vec(), [-1].to_vec());
                }
                total_length -= widths[c as usize];
                pruned_columns.insert(c);
                if offset >= 0 {
                    offset = -offset - 1;
                } else {
                    offset = -offset;
                }
            }
        }
    }
    let mut added_split_column = false;
    let mut new_widths = vec![];
    let mut column_map = vec![];
    for (c, item) in widths.iter().enumerate().take(column_count) {
        if !pruned_columns.contains(&(c as i32)) {
            column_map.push((c).try_into().unwrap());
            new_widths.push(*item);
        } else if !added_split_column {
            // "..."
            column_map.push(-1);
            new_widths.push(3);
            added_split_column = true;
        }
    }

    (new_widths, column_map)
}

fn render_head(
    schema: &DataSchemaRef,
    widths: &mut [usize],
    column_map: &mut [i32],
    header: &mut Vec<Cell>,
    aligns: &mut Vec<CellAlignment>,
) {
    if column_map.is_empty() {
        for field in schema.fields() {
            let cell = Cell::new(format!("{}\n{}", field.name(), field.data_type(),))
                .set_alignment(CellAlignment::Center);

            header.push(cell);

            if field.data_type().is_numeric() {
                aligns.push(CellAlignment::Right);
            } else {
                aligns.push(CellAlignment::Left);
            }
        }
    } else {
        let fields = schema.fields();
        for (i, col_index) in column_map.iter().enumerate() {
            if *col_index == -1 {
                let cell = Cell::new("···").set_alignment(CellAlignment::Center);
                header.push(cell);
                aligns.push(CellAlignment::Center);
            } else {
                let field = &fields[*col_index as usize];
                let width = widths[i];
                let mut field_name = field.name().to_string();
                let mut field_data_type = field.data_type().to_string();
                let element_size = if width >= 6 { width - 6 } else { 0 };

                if field_name.len() + 3 > width {
                    field_name = String::from_utf8(
                        field_name
                            .graphemes(true)
                            .take(element_size)
                            .flat_map(|g| g.as_bytes().iter())
                            .copied() // copied converts &u8 into u8
                            .chain(b"...".iter().copied())
                            .collect::<Vec<u8>>(),
                    )
                    .unwrap();
                }
                if field_data_type.len() + 3 > width {
                    field_data_type = String::from_utf8(
                        field_name
                            .graphemes(true)
                            .take(element_size)
                            .flat_map(|g| g.as_bytes().iter())
                            .copied() // copied converts &u8 into u8
                            .chain(b"...".iter().copied())
                            .collect::<Vec<u8>>(),
                    )
                    .unwrap();
                }

                let cell = Cell::new(format!("{}\n{}", field_name, field_data_type))
                    .set_alignment(CellAlignment::Center);

                header.push(cell);

                if field.data_type().is_numeric() {
                    aligns.push(CellAlignment::Right);
                } else {
                    aligns.push(CellAlignment::Left);
                }
            }
        }
    }
}

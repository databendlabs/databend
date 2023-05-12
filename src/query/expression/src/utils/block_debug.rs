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

pub fn table_format_blocks(schema: &DataSchemaRef, blocks: &[DataBlock]) -> Result<String> {
    let table = create_table(schema, blocks);
    Ok(table.to_string())
}

/// Convert a series of rows into a table
fn create_table(schema: &DataSchemaRef, results: &[DataBlock]) -> Table {
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

    for block in results {
        for row in 0..block.num_rows() {
            let mut cells = Vec::new();
            for (align, block_entry) in aligns.iter().zip(block.columns()) {
                let cell = Cell::new(block_entry.value.index(row).unwrap()).set_alignment(*align);
                cells.push(cell);
            }
            table.add_row(cells);
        }
    }

    table
}

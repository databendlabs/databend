// Copyright 2023 Datafuse Labs.
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

use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use comfy_table::{Cell, CellAlignment, Table};

use arrow_cast::display::{ArrayFormatter, FormatOptions};

use crate::session::QueryKind;

/// Prints a visual representation of record batches to stdout
pub fn print_batches(query_kind: QueryKind, results: &[RecordBatch]) -> Result<(), ArrowError> {
    let options = FormatOptions::default().with_display_error(true);

    println!("{}", create_table(query_kind, results, &options)?);
    Ok(())
}

/// Convert a series of record batches into a table
fn create_table(
    query_kind: QueryKind,
    results: &[RecordBatch],
    options: &FormatOptions,
) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("││──├─┼┤│    ──┌┐└┘");
    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        let cell = Cell::new(format!(
            "{}\n{}",
            field.name(),
            normalize_datatype(field.data_type())
        ))
        .set_alignment(CellAlignment::Center);
        header.push(cell);
    }
    table.set_header(header);

    let align = match query_kind {
        QueryKind::Query => CellAlignment::Right,
        _ => CellAlignment::Left,
    };

    for batch in results {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for formatter in &formatters {
                let cell = Cell::new(formatter.value(row)).set_alignment(align);
                cells.push(cell);
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

// LargeUtf8 --> String
fn normalize_datatype(ty: &DataType) -> String {
    match ty {
        DataType::LargeUtf8 => "String".to_owned(),
        _ => format!("{ty}"),
    }
}

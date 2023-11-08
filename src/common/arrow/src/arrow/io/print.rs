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

//! APIs to represent [`Chunk`] as a formatted table.

use comfy_table::Cell;
use comfy_table::Table;

use crate::arrow::array::get_display;
use crate::arrow::array::Array;
use crate::arrow::chunk::Chunk;

/// Returns a visual representation of [`Chunk`]
pub fn write<A: AsRef<dyn Array>, N: AsRef<str>>(chunks: &[Chunk<A>], names: &[N]) -> String {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if chunks.is_empty() {
        return table.to_string();
    }

    let header = names.iter().map(|name| Cell::new(name.as_ref()));
    table.set_header(header);

    for chunk in chunks {
        let displays = chunk
            .arrays()
            .iter()
            .map(|array| get_display(array.as_ref(), ""))
            .collect::<Vec<_>>();

        for row in 0..chunk.len() {
            let mut cells = Vec::new();
            (0..chunk.arrays().len()).for_each(|col| {
                let mut string = String::new();
                displays[col](&mut string, row).unwrap();
                cells.push(Cell::new(string));
            });
            table.add_row(cells);
        }
    }
    table.to_string()
}

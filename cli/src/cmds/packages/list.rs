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

use std::fs;

use clap::ArgMatches;
use comfy_table::Cell;
use comfy_table::CellAlignment;
use comfy_table::Color;
use comfy_table::Table;

use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct ListCommand {
    conf: Config,
}

impl ListCommand {
    pub fn create(conf: Config) -> Self {
        ListCommand { conf }
    }
    pub fn exec_match(&self, writer: &mut Writer, _args: Option<&ArgMatches>) -> Result<()> {
        let bin_dir = format!("{}/bin", self.conf.datafuse_dir.clone());
        let paths = fs::read_dir(bin_dir)?;

        // Status.
        let mut current = "".to_string();
        if let Ok(status) = Status::read(self.conf.clone()) {
            current = status.version;
        }

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        // Title.
        table.set_header(vec![
            Cell::new("Version"),
            Cell::new("Path"),
            Cell::new("Current"),
        ]);
        for path in paths {
            let path = path.unwrap();
            let version = path
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let mut row = vec![];
            row.push(Cell::new(version.as_str()));
            row.push(Cell::new(format!("{}", path.path().display(),)));

            let mut current_marker = Cell::new("");
            if current == version {
                current_marker = Cell::new("☑").fg(Color::Green);
            }
            row.push(current_marker.set_alignment(CellAlignment::Center));
            table.add_row(row);
        }
        writer.writeln(&table.trim_fmt());

        Ok(())
    }
}

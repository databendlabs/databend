// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs;

use colored::Colorize;
use prettytable::Cell;
use prettytable::Row;
use prettytable::Table;

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

    pub fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        let bin_dir = format!("{}/bin", self.conf.datafuse_dir.clone());
        let paths = fs::read_dir(bin_dir)?;

        // Status.
        let mut current = "".to_string();
        if let Ok(status) = Status::read(self.conf.clone()) {
            current = status.version;
        }

        let mut table = Table::new();
        // Title.
        table.add_row(Row::new(vec![
            Cell::new("Version"),
            Cell::new("Path"),
            Cell::new("Current"),
        ]));
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
            row.push(Cell::new(format!("{}", path.path().display(),).as_str()));

            let mut current_marker = "".to_string();
            if current == version {
                current_marker = format!("{}", "✅ ".blue());
            }
            row.push(Cell::new(current_marker.as_str()));
            table.add_row(Row::new(row));
        }
        table.print(writer).unwrap();

        Ok(())
    }
}

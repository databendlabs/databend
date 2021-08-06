// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs;

use prettytable::Cell;
use prettytable::Row;
use prettytable::Table;

use crate::cmds::Config;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct SwitchCommand {
    conf: Config,
}

impl SwitchCommand {
    pub fn create(conf: Config) -> Self {
        SwitchCommand { conf }
    }

    pub fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        let bin_dir = format!("{}/bin", self.conf.datafuse_dir.clone());
        let paths = fs::read_dir(bin_dir)?;

        let mut table = Table::new();
        // Title.
        table.add_row(Row::new(vec![Cell::new("Version"), Cell::new("Path")]));
        for path in paths {
            let path = path.unwrap();
            let mut row = vec![];
            row.push(Cell::new(
                path.path().file_name().unwrap().to_str().unwrap(),
            ));
            row.push(Cell::new(format!("{}", path.path().display(),).as_str()));
            table.add_row(Row::new(row));
        }
        table.print(writer).unwrap();

        Ok(())
    }
}

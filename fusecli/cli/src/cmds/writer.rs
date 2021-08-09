// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Stdout;
use std::io::Write;

use colored::Colorize;

enum WriterOutput {
    Stdout(Stdout),
}

pub struct Writer {
    output: WriterOutput,
}

impl Writer {
    pub fn create() -> Self {
        Writer {
            output: WriterOutput::Stdout(std::io::stdout()),
        }
    }

    pub fn writeln_width(&mut self, name: &str, value: &str) {
        let width = 20;
        writeln!(self, "{:width$} {}", name, value, width = width).unwrap();
    }

    pub fn writeln(&mut self, value: &str) {
        writeln!(self, "{}", value).unwrap();
    }

    pub fn write_ok(&mut self, msg: &str) {
        writeln!(self, "✅ {} {}", "[ok]".bold().green(), msg).unwrap();
    }

    pub fn write_err(&mut self, msg: &str) {
        writeln!(self, "{} {}", "[failed]".bold().red(), msg.red()).unwrap();
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.output {
            WriterOutput::Stdout(ref mut w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.output {
            WriterOutput::Stdout(ref mut w) => w.flush(),
        }
    }
}

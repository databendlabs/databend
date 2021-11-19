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

use std::io::Stdout;
use std::io::Write;

use colored::Colorize;

enum WriterOutput {
    Stdout(Stdout),
}

pub struct Writer {
    output: WriterOutput,
    pub(crate) debug: bool,
}

impl Writer {
    pub fn create() -> Self {
        Writer {
            output: WriterOutput::Stdout(std::io::stdout()),
            debug: false,
        }
    }

    pub fn writeln_width(&mut self, name: &str, value: &str) {
        let width = 20;
        writeln!(self, "{:width$} {}", name, value, width = width).unwrap();
    }

    pub fn writeln(&mut self, value: &str) {
        writeln!(self, "{}", value).unwrap();
    }

    pub fn write_ok(&mut self, msg: String) {
        writeln!(self, "{} ✅ {}", "[ok]".bold().green(), msg).unwrap();
    }

    pub fn write_warn(&mut self, msg: String) {
        writeln!(self, "{} ⚠ {}", "[ok]".bold().green(), msg).unwrap();
    }

    pub fn write_rocket(&mut self, msg: String) {
        writeln!(self, "{} 🚀 {}", "[ok]".bold().green(), msg).unwrap();
    }

    pub fn write_debug(&mut self, msg: String) {
        if self.debug {
            writeln!(self, "{} {}", "[debug]".bold().yellow(), msg).unwrap();
        }
    }

    pub fn write_err(&mut self, msg: String) {
        writeln!(self, "{} ❌ {}", "[failed]".bold().red(), msg.red()).unwrap();
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

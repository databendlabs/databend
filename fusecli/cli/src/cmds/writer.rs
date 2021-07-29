// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Stdout;
use std::io::Write;

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

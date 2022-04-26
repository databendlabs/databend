// Copyright 2021 Datafuse Labs.
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

use std::io;
use std::io::Result;

use super::checkpoint_read::CheckpointRead;
use super::BufferRead;

// This is dynamic checkpoint buffer reader
pub type CpBufferReader<'a> = CheckpointReader<Box<dyn BufferRead + Send + 'a>>;

pub struct CheckpointReader<R: BufferRead> {
    reader: R,
    buffer: Vec<u8>,

    checkpoint_enable: bool,
}

impl<R: BufferRead> CheckpointReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::new(),
            checkpoint_enable: false,
        }
    }
}

impl<R: BufferRead> CheckpointRead for CheckpointReader<R> {
    // reset the checkpoint
    fn reset_checkpoint(&mut self) {
        self.buffer.clear();
        self.checkpoint_enable = false;
    }

    fn checkpoint(&mut self) {
        self.buffer.clear();
        self.checkpoint_enable = true;
    }

    fn get_checkpoint_buffer(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    fn rollback_to_checkpoint(&mut self) -> Result<()> {
        let buffer = std::mem::replace(&mut self.buffer, Vec::with_capacity(0));
        self.reader.preadd_buf(&buffer)?;
        Ok(())
    }
}

impl<R: BufferRead> io::Read for CheckpointReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.checkpoint_enable {
            false => self.reader.read(buf),
            true => {
                let size = self.reader.read(buf)?;
                self.buffer.extend_from_slice(&buf[0..size]);
                Ok(size)
            }
        }
    }
}

impl<R: BufferRead> BufferRead for CheckpointReader<R> {
    fn working_buf(&self) -> &[u8] {
        self.reader.working_buf()
    }

    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        let buffer = self.reader.working_buf();
        self.buffer
            .extend_from_slice(&buffer[0..amt.min(buffer.len())]);
        self.reader.consume(amt)
    }

    fn preadd_buf(&mut self, buf: &[u8]) -> Result<()> {
        self.reader.preadd_buf(buf)
    }
}

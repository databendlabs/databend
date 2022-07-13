// Copyright 2022 Datafuse Labs.
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

use super::BufferRead;

// this struct can avoid NestedCheckpointReader<NestedCheckpointReader<R>> in recursive functions
pub struct NestedCheckpointReader<R: BufferRead> {
    reader: R,
    // clear buffer until all checkpoints is popped and the buffer is consumed
    buffer: Vec<u8>,
    checkpoints: Vec<usize>,

    checkpointing: bool,
    pub pos: usize,
}

impl<R: BufferRead> NestedCheckpointReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::new(),
            checkpoints: Vec::new(),
            checkpointing: false,
            pos: 0,
        }
    }

    pub fn push_checkpoint(&mut self) {
        self.checkpointing = true;
        self.checkpoints.push(self.pos)
    }

    // todo(youngsofun): add CheckPointGuard to make it safer.
    pub fn pop_checkpoint(&mut self) {
        assert!(self.checkpointing);
        self.checkpoints.pop();
    }

    pub fn rollback_to_checkpoint(&mut self) -> Result<()> {
        self.pos = self.checkpoints[self.checkpoints.len() - 1];
        Ok(())
    }

    pub fn get_checkpoint_buffer(&self) -> &[u8] {
        let cp = self.checkpoints[self.checkpoints.len() - 1];
        &self.buffer[cp..self.pos]
    }

    pub fn get_checkpoint_buffer_end(&mut self) -> &[u8] {
        let cp = self.checkpoints[self.checkpoints.len() - 1];
        self.fill_buf().unwrap();
        &self.buffer[cp..self.buffer.len()]
    }

    pub fn get_top_checkpoint_pos(&self) -> usize {
        if self.checkpointing {
            return self.checkpoints[self.checkpoints.len() - 1];
        }
        0
    }

    pub fn clear(&mut self) {
        self.checkpointing = false;
        self.buffer.clear();
        self.pos = 0;
    }
}

impl<R: BufferRead> io::Read for NestedCheckpointReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.checkpointing {
            false => self.reader.read(buf),
            true => {
                if self.pos < self.buffer.len() {
                    let buffer = &self.buffer[self.pos..self.buffer.len()];
                    let n_read = buffer.len().min(buf.len());
                    buf[..n_read].copy_from_slice(&buffer[..n_read]);
                    self.pos += n_read;
                    Ok(n_read)
                } else {
                    if self.checkpoints.is_empty() {
                        self.clear();
                    }
                    let n_read = self.reader.read(buf)?;
                    self.buffer.extend_from_slice(&buf[0..n_read]);
                    self.pos += n_read;
                    Ok(n_read)
                }
            }
        }
    }
}

impl<R: BufferRead> BufferRead for NestedCheckpointReader<R> {
    fn working_buf(&self) -> &[u8] {
        unreachable!()
    }

    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self.checkpointing {
            false => {
                let inner = self.reader.fill_buf()?;
                Ok(inner)
            }
            true => {
                if self.pos == self.buffer.len() {
                    let inner = self.reader.fill_buf()?;
                    self.buffer.extend_from_slice(inner);
                    let size = inner.len();
                    self.reader.consume(size);
                }
                Ok(&self.buffer[self.pos..self.buffer.len()])
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        match self.checkpointing {
            false => self.reader.consume(amt),
            true => {
                self.pos += amt;
                assert!(self.pos <= self.buffer.len())
            }
        }
    }

    fn preadd_buf(&mut self, _buf: &[u8]) -> Result<()> {
        unreachable!()
    }
}

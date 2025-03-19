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

use std::fmt;
use std::io;
use std::io::Error;
use std::io::Read;
use std::io::Write;

use codeq::Decode;
use codeq::Encode;

use crate::CURRENT_VERSION;

/// The semaphore entry is used to represent the semaphore state in the meta-service.
///
/// It contains the user defined id and the value(how many permits the semaphore requires).
///
/// It is the value payload of the semaphore key in the meta-service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemaphoreEntry {
    pub id: String,
    pub value: u64,
}

impl Encode for SemaphoreEntry {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        n += CURRENT_VERSION.encode(&mut w)?;
        n += self.id.encode(&mut w)?;
        n += self.value.encode(&mut w)?;
        Ok(n)
    }
}

impl Decode for SemaphoreEntry {
    fn decode<R: Read>(mut r: R) -> Result<Self, io::Error> {
        let version = u8::decode(&mut r)?;
        if version != CURRENT_VERSION {
            return Err(Error::new(io::ErrorKind::InvalidData, "Invalid version"));
        }
        let id = Decode::decode(&mut r)?;
        let value = Decode::decode(&mut r)?;

        Ok(SemaphoreEntry { id, value })
    }
}

impl fmt::Display for SemaphoreEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SemEntry(id:{}, value:{})", self.id, self.value)
    }
}

impl SemaphoreEntry {
    pub fn new(id: impl ToString, value: u64) -> Self {
        Self {
            id: id.to_string(),
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test the display of SemaphoreEntry
    #[test]
    fn test_display() {
        let entry = SemaphoreEntry::new("test", 1);
        assert_eq!(entry.to_string(), "SemaphoreEntry(id:test, value:1)");
    }

    // Test the encode and decode of SemaphoreEntry
    #[test]
    fn test_encode_decode() {
        let entry = SemaphoreEntry::new("test", 1);
        let buf = entry.encode_to_vec().unwrap();

        assert_eq!(buf.len(), 17);
        assert_eq!(
            vec![
                1, // version
                0, 0, 0, 4, // string len
                116, 101, 115, 116, // string "test"
                0, 0, 0, 0, 0, 0, 0, 1 // value
            ],
            buf
        );

        let decoded = SemaphoreEntry::decode(&mut &buf[..]).unwrap();
        assert_eq!(entry, decoded);
    }
}

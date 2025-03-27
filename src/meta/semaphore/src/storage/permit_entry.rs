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

/// A [`PermitEntry`] represents a semaphore permit in the meta-service.
///
/// It contains the user defined id and the number of permits.
///
/// It is the value payload of the [`PermitKey`](crate::storage::PermitKey) in the meta-service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermitEntry {
    pub id: String,
    pub permits: u64,
}

impl Encode for PermitEntry {
    fn encode<W: Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut n = 0;
        n += CURRENT_VERSION.encode(&mut w)?;
        n += self.id.encode(&mut w)?;
        n += self.permits.encode(&mut w)?;
        Ok(n)
    }
}

impl Decode for PermitEntry {
    fn decode<R: Read>(mut r: R) -> Result<Self, io::Error> {
        let version = u8::decode(&mut r)?;
        if version != CURRENT_VERSION {
            return Err(Error::new(io::ErrorKind::InvalidData, "Invalid version"));
        }
        let id = Decode::decode(&mut r)?;
        let permits = Decode::decode(&mut r)?;

        Ok(PermitEntry { id, permits })
    }
}

impl fmt::Display for PermitEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PermitEntry(id:{}, n:{})", self.id, self.permits)
    }
}

impl PermitEntry {
    pub fn new(id: impl ToString, value: u64) -> Self {
        Self {
            id: id.to_string(),
            permits: value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test the display of PermitEntry
    #[test]
    fn test_display() {
        let entry = PermitEntry::new("test", 1);
        assert_eq!(entry.to_string(), "PermitEntry(id:test, n:1)");
    }

    // Test the encode and decode of PermitEntry
    #[test]
    fn test_encode_decode() {
        let entry = PermitEntry::new("test", 1);
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

        let decoded = PermitEntry::decode(&mut &buf[..]).unwrap();
        assert_eq!(entry, decoded);
    }
}

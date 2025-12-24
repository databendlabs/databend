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

use std::io;
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Seek;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;

// Sizes of header structures
const DESCRIPTION_BYTES: usize = 4;
const OFFSET_BYTES: usize = 4;

#[derive(Clone)]
pub struct TreemapReader<'a> {
    buf: &'a [u8],
    _size: u64,
}

impl<'a> TreemapReader<'a> {
    pub fn new(mut buf: &'a [u8]) -> io::Result<Self> {
        let size = buf
            .read_u64::<LittleEndian>()
            .map_err(|_| Error::other("fail to read size"))?;

        Ok(Self { buf, _size: size })
    }

    pub fn iter(&self) -> TreeMapIter<'_> {
        TreeMapIter {
            buf: self.buf,
            offset: 0,
        }
    }
}

pub struct TreeMapIter<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for TreeMapIter<'a> {
    type Item = io::Result<BitmapReader<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == self.offset {
            return None;
        }

        match BitmapReader::decode(&self.buf[self.offset..]) {
            Ok(header) => {
                self.offset += header.buf.len();
                Some(Ok(header))
            }
            Err(err) => {
                self.offset = self.buf.len();
                Some(Err(err))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BitmapReader<'a> {
    prefix: u32,
    containers: u32,
    buf: &'a [u8],
}

impl BitmapReader<'_> {
    pub fn decode(buf: &[u8]) -> io::Result<BitmapReader<'_>> {
        let mut reader = Cursor::new(buf);
        let prefix = reader.read_u32::<LittleEndian>()?;

        const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
        const SERIAL_COOKIE: u16 = 12347;

        // First read the cookie to determine which version of the format we are reading
        let containers = {
            let cookie = reader.read_u32::<LittleEndian>()?;
            if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
                reader.read_u32::<LittleEndian>()?
            } else if (cookie as u16) == SERIAL_COOKIE {
                return Err(Error::other("does not support run containers"));
            } else {
                return Err(Error::other("unknown cookie value"));
            }
        };

        if containers > u16::MAX as u32 + 1 {
            return Err(Error::other("size is greater than supported"));
        }

        let last_container = (containers - 1) as i64;
        reader.seek_relative(last_container * DESCRIPTION_BYTES as i64 + 2)?;
        let last_cardinality = reader.read_u16::<LittleEndian>()? as usize + 1;

        reader.seek_relative(last_container * OFFSET_BYTES as i64)?;
        let last_offset = reader.read_u32::<LittleEndian>()?;

        const ARRAY_LIMIT: usize = 4096;
        const BITMAP_LENGTH: usize = 1024;

        let size = 4
            + last_offset as usize
            + if last_cardinality < ARRAY_LIMIT {
                2 * last_cardinality
            } else {
                BITMAP_LENGTH
            };

        if buf.len() < size {
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                "data is truncated or invalid",
            ))
        } else {
            Ok(BitmapReader {
                prefix,
                containers,
                buf: &buf[..size],
            })
        }
    }

    pub fn containers(&self) -> usize {
        self.containers as usize
    }

    #[allow(dead_code)]
    pub fn prefix(&self) -> u32 {
        self.prefix
    }

    pub fn description(&self, i: usize) -> io::Result<Description> {
        if i >= self.containers() {
            return Err(Error::new(ErrorKind::InvalidInput, "index out of range"));
        }

        let mut desc_buf = &self.buf[12 + i * DESCRIPTION_BYTES..];
        let prefix = desc_buf.read_u16::<LittleEndian>()?;
        let cardinality = desc_buf.read_u16::<LittleEndian>()?;
        Ok(Description {
            prefix,
            cardinality,
        })
    }
}

pub struct Description {
    #[allow(dead_code)]
    pub prefix: u16,
    cardinality: u16,
}

impl Description {
    pub fn cardinality(&self) -> usize {
        self.cardinality as usize + 1
    }
}

pub fn bitmap_len(buf: &[u8]) -> io::Result<usize> {
    let tree = TreemapReader::new(buf)?;
    let mut sum = 0;
    for bitmap in tree.iter() {
        let bitmap = bitmap?;
        for i in 0..bitmap.containers() {
            sum += bitmap.description(i)?.cardinality();
        }
    }
    Ok(sum)
}

#[cfg(test)]
mod tests {

    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use roaring::RoaringTreemap;

    use super::*;

    #[test]
    fn test_base() -> io::Result<()> {
        let mut rng = SmallRng::seed_from_u64(123);

        let mut map = RoaringTreemap::new();
        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            map.insert(v);
        }

        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            map.insert(v & u32::MAX as u64);
        }

        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            map.insert(v & u16::MAX as u64);
        }

        let mut buf = Vec::new();
        map.serialize_into(&mut buf)?;

        assert_eq!(bitmap_len(&buf)?, 150);

        Ok(())
    }
}

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
use roaring::RoaringTreemap;

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

    pub fn contains(&self, value: u64) -> io::Result<bool> {
        let high = (value >> 32) as u32;
        let low = (value & 0xFFFFFFFF) as u32;
        for bitmap_res in self.iter() {
            let bitmap = bitmap_res?;
            if bitmap.prefix() == high {
                return bitmap.contains(low);
            } else if bitmap.prefix() > high {
                break;
            }
        }
        Ok(false)
    }

    pub fn max(&self) -> io::Result<Option<u64>> {
        let mut last_bitmap = None;
        for bitmap_res in self.iter() {
            last_bitmap = Some(bitmap_res?);
        }
        if let Some(bitmap) = last_bitmap {
            let high = bitmap.prefix() as u64;
            let low = bitmap.max()? as u64;
            Ok(Some((high << 32) | low))
        } else {
            Ok(None)
        }
    }

    pub fn min(&self) -> io::Result<Option<u64>> {
        if let Some(bitmap_res) = self.iter().next() {
            let bitmap = bitmap_res?;
            let high = bitmap.prefix() as u64;
            let low = bitmap.min()? as u64;
            Ok(Some((high << 32) | low))
        } else {
            Ok(None)
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
        const BITMAP_LENGTH: usize = 8192;

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

    pub fn bitmap_buf(&self) -> &[u8] {
        &self.buf[4..]
    }

    pub fn contains(&self, value: u32) -> io::Result<bool> {
        let high = (value >> 16) as u16;
        let low = (value & 0xFFFF) as u16;

        for i in 0..self.containers() {
            let desc = self.description(i)?;
            if desc.prefix == high {
                let offset_pos = 12 + self.containers() * 4 + i * 4;
                if offset_pos + 4 > self.buf.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "offset out of bounds"));
                }
                let mut offset_bytes = &self.buf[offset_pos..offset_pos + 4];
                let offset = offset_bytes.read_u32::<LittleEndian>()? as usize;

                let container_start = 4 + offset;
                let cardinality = desc.cardinality();

                const ARRAY_LIMIT: usize = 4096;
                if cardinality <= ARRAY_LIMIT {
                    let container_len = cardinality * 2;
                    if container_start + container_len > self.buf.len() {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "container out of bounds",
                        ));
                    }
                    let container_buf = &self.buf[container_start..container_start + container_len];

                    let mut l = 0;
                    let mut r = cardinality;
                    while l < r {
                        let mid = (l + r) / 2;
                        let val_pos = mid * 2;
                        let mut val_bytes = &container_buf[val_pos..val_pos + 2];
                        let val = val_bytes.read_u16::<LittleEndian>()?;
                        match val.cmp(&low) {
                            std::cmp::Ordering::Equal => return Ok(true),
                            std::cmp::Ordering::Less => l = mid + 1,
                            std::cmp::Ordering::Greater => r = mid,
                        }
                    }
                    return Ok(false);
                } else {
                    const BITMAP_LENGTH: usize = 8192;
                    if container_start + BITMAP_LENGTH > self.buf.len() {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "container out of bounds",
                        ));
                    }
                    let container_buf = &self.buf[container_start..container_start + BITMAP_LENGTH];
                    let byte_idx = (low >> 3) as usize;
                    let bit_idx = (low & 7) as u8;
                    let byte = container_buf[byte_idx];
                    return Ok((byte & (1 << bit_idx)) != 0);
                }
            } else if desc.prefix > high {
                break;
            }
        }
        Ok(false)
    }

    pub fn max(&self) -> io::Result<u32> {
        let containers = self.containers();
        if containers == 0 {
            return Err(Error::new(ErrorKind::InvalidData, "empty bitmap"));
        }
        let last_idx = containers - 1;
        let desc = self.description(last_idx)?;
        let high = desc.prefix;

        let offset_pos = 12 + containers * 4 + last_idx * 4;
        if offset_pos + 4 > self.buf.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "offset out of bounds"));
        }
        let mut offset_bytes = &self.buf[offset_pos..offset_pos + 4];
        let offset = offset_bytes.read_u32::<LittleEndian>()? as usize;
        let container_start = 4 + offset;
        let cardinality = desc.cardinality();

        const ARRAY_LIMIT: usize = 4096;
        if cardinality <= ARRAY_LIMIT {
            let last_val_pos = container_start + (cardinality - 1) * 2;
            if last_val_pos + 2 > self.buf.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "container out of bounds",
                ));
            }
            let mut val_bytes = &self.buf[last_val_pos..last_val_pos + 2];
            let low = val_bytes.read_u16::<LittleEndian>()?;
            Ok(((high as u32) << 16) | (low as u32))
        } else {
            const BITMAP_LENGTH: usize = 8192;
            if container_start + BITMAP_LENGTH > self.buf.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "container out of bounds",
                ));
            }
            let container_buf = &self.buf[container_start..container_start + BITMAP_LENGTH];
            for idx in (0..BITMAP_LENGTH).rev() {
                let byte = container_buf[idx];
                if byte != 0 {
                    let bit_idx = 7 - byte.leading_zeros() as u16;
                    let low = (idx as u32 * 8) + bit_idx as u32;
                    return Ok(((high as u32) << 16) | low);
                }
            }
            Err(Error::new(
                ErrorKind::InvalidData,
                "invalid empty bitmap container",
            ))
        }
    }

    pub fn min(&self) -> io::Result<u32> {
        let containers = self.containers();
        if containers == 0 {
            return Err(Error::new(ErrorKind::InvalidData, "empty bitmap"));
        }
        let desc = self.description(0)?;
        let high = desc.prefix;

        let offset_pos = 12 + containers * 4;
        if offset_pos + 4 > self.buf.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "offset out of bounds"));
        }
        let mut offset_bytes = &self.buf[offset_pos..offset_pos + 4];
        let offset = offset_bytes.read_u32::<LittleEndian>()? as usize;
        let container_start = 4 + offset;
        let cardinality = desc.cardinality();

        const ARRAY_LIMIT: usize = 4096;
        if cardinality <= ARRAY_LIMIT {
            if container_start + 2 > self.buf.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "container out of bounds",
                ));
            }
            let mut val_bytes = &self.buf[container_start..container_start + 2];
            let low = val_bytes.read_u16::<LittleEndian>()?;
            Ok(((high as u32) << 16) | (low as u32))
        } else {
            const BITMAP_LENGTH: usize = 8192;
            if container_start + BITMAP_LENGTH > self.buf.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "container out of bounds",
                ));
            }
            let container_buf = &self.buf[container_start..container_start + BITMAP_LENGTH];
            for idx in 0..BITMAP_LENGTH {
                let byte = container_buf[idx];
                if byte != 0 {
                    let bit_idx = byte.trailing_zeros() as u16;
                    let low = (idx as u32 * 8) + bit_idx as u32;
                    return Ok(((high as u32) << 16) | low);
                }
            }
            Err(Error::new(
                ErrorKind::InvalidData,
                "invalid empty bitmap container",
            ))
        }
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

pub fn intersection_with_serialized(tree: &mut RoaringTreemap, buf: &[u8]) -> io::Result<()> {
    use std::cmp::Ordering::*;
    let rhs = TreemapReader::new(buf)?;
    let mut bitmaps = Vec::new();
    let mut lhs_iter = tree.bitmaps();
    let mut rhs_iter = rhs.iter();

    let mut lhs_curr = lhs_iter.next();
    let mut rhs_curr = rhs_iter.next().transpose()?;

    while let (Some((lhs_prefix, lhs_bitmap)), Some(rhs_bitmap)) = (lhs_curr, rhs_curr.as_ref()) {
        match lhs_prefix.cmp(&rhs_bitmap.prefix()) {
            Less => {
                lhs_curr = lhs_iter.next();
            }
            Greater => {
                rhs_curr = rhs_iter.next().transpose()?;
            }
            Equal => {
                let intersection = lhs_bitmap
                    .intersection_with_serialized_unchecked(Cursor::new(rhs_bitmap.bitmap_buf()))?;
                if !intersection.is_empty() {
                    bitmaps.push((lhs_prefix, intersection));
                }
                lhs_curr = lhs_iter.next();
                rhs_curr = rhs_iter.next().transpose()?;
            }
        }
    }

    *tree = RoaringTreemap::from_bitmaps(bitmaps);

    Ok(())
}

#[cfg(test)]
mod tests {

    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use roaring::RoaringTreemap;

    use super::*;

    fn create_bitmap(seed: u64) -> RoaringTreemap {
        let mut rng = SmallRng::seed_from_u64(seed);

        let mut bitmap = RoaringTreemap::new();
        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            bitmap.insert(v);
        }

        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            bitmap.insert(v & u32::MAX as u64);
        }

        for _ in 0..50 {
            let v = rng.r#gen::<u64>();
            bitmap.insert(v & u16::MAX as u64);
        }

        bitmap
    }

    #[test]
    fn test_len() -> io::Result<()> {
        let bitmap = create_bitmap(123);
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf)?;
        assert_eq!(bitmap_len(&buf)?, 150);

        Ok(())
    }

    #[test]
    fn test_intersection() -> io::Result<()> {
        let v1 = create_bitmap(123);
        let v2 = create_bitmap(456);
        let v3 = create_bitmap(789);

        let v12 = &v1 | &v2;
        let v23 = &v2 | &v3;

        assert_eq!(&v12 & &v23, v2);

        let mut buf = Vec::new();
        v23.serialize_into(&mut buf)?;

        let mut v = v12;
        intersection_with_serialized(&mut v, &buf)?;

        assert_eq!(v, v2);

        Ok(())
    }

    #[test]
    fn test_contains_max_min() -> io::Result<()> {
        let bitmap = create_bitmap(123);
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf)?;

        let reader = TreemapReader::new(&buf)?;

        // Verify min and max bounds
        assert_eq!(reader.min()?, bitmap.min());
        assert_eq!(reader.max()?, bitmap.max());

        // Verify all present values
        for val in &bitmap {
            assert!(reader.contains(val)?, "bitmap should contain {}", val);
        }

        // Verify some absent values
        let mut rng = SmallRng::seed_from_u64(999);
        for _ in 0..100 {
            let val = rng.r#gen::<u64>();
            if !bitmap.contains(val) {
                assert!(!reader.contains(val)?, "bitmap should NOT contain {}", val);
            }
        }

        // Verify empty bitmap
        let empty_bitmap = RoaringTreemap::new();
        let mut empty_buf = Vec::new();
        empty_bitmap.serialize_into(&mut empty_buf)?;
        let empty_reader = TreemapReader::new(&empty_buf)?;
        assert_eq!(empty_reader.min()?, None);
        assert_eq!(empty_reader.max()?, None);
        assert!(!empty_reader.contains(42)?);

        Ok(())
    }
}

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

// Container layout constants
const ARRAY_LIMIT: usize = 4096;
const WORD_BITS: usize = 64;
const WORD_BYTES: usize = 8;
const BITMAP_WORDS: usize = 1024;
const BITMAP_BYTES: usize = WORD_BYTES * BITMAP_WORDS;

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

    pub(crate) fn container_offset(&self, i: usize) -> io::Result<usize> {
        if i >= self.containers() {
            return Err(Error::other("index out of range"));
        }
        let offset_table_start = 12 + self.containers() * DESCRIPTION_BYTES;
        let offset_pos = offset_table_start + i * OFFSET_BYTES;
        if offset_pos + OFFSET_BYTES > self.buf.len() {
            return Err(Error::other("offset table too short"));
        }
        let mut reader = Cursor::new(&self.buf[offset_pos..]);
        let offset = reader.read_u32::<LittleEndian>()? as usize;
        if offset > self.bitmap_buf().len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "container offset exceeds bitmap data",
            ));
        }
        Ok(offset)
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

pub(crate) fn bitmap_contains(buf: &[u8], value: u64) -> io::Result<bool> {
    let tree = TreemapReader::new(buf)?;
    let prefix = (value >> 32) as u32;
    let container_key = ((value >> 16) & 0xFFFF) as u16;
    let low16 = (value & 0xFFFF) as u16;

    for bitmap_result in tree.iter() {
        let bitmap = bitmap_result?;
        if bitmap.prefix() < prefix {
            continue;
        }
        if bitmap.prefix() > prefix {
            return Ok(false);
        }
        return bitmap_contains_in_bitmap(&bitmap, container_key, low16);
    }
    Ok(false)
}

fn bitmap_contains_in_bitmap(
    bitmap: &BitmapReader<'_>,
    container_key: u16,
    low16: u16,
) -> io::Result<bool> {
    let mut lo = 0;
    let mut hi = bitmap.containers();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let desc = bitmap.description(mid)?;
        match desc.prefix.cmp(&container_key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                let cardinality = desc.cardinality();
                let offset = bitmap.container_offset(mid)?;
                let container_data = &bitmap.bitmap_buf()[offset..];
                return if cardinality < ARRAY_LIMIT {
                    array_container_contains(container_data, cardinality, low16)
                } else {
                    bitmap_container_contains(container_data, low16)
                };
            }
        }
    }
    Ok(false)
}

/// Binary search in an array container (cardinality < 4096).
fn array_container_contains(data: &[u8], cardinality: usize, low16: u16) -> io::Result<bool> {
    if data.len() < cardinality * 2 {
        return Err(Error::other("array container too short"));
    }
    let mut lo = 0;
    let mut hi = cardinality;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let v = u16::from_le_bytes(data[mid * 2..mid * 2 + 2].try_into().unwrap());
        match v.cmp(&low16) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

/// Bit lookup in a bitmap container (cardinality >= 4096).
fn bitmap_container_contains(data: &[u8], low16: u16) -> io::Result<bool> {
    if data.len() < BITMAP_BYTES {
        return Err(Error::other("bitmap container too short"));
    }
    let word_index = low16 as usize / WORD_BITS;
    let bit_index = low16 as usize % WORD_BITS;
    let start = word_index * WORD_BYTES;
    let word = u64::from_le_bytes(data[start..start + WORD_BYTES].try_into().unwrap());
    Ok(word & (1 << bit_index) != 0)
}

pub(crate) fn bitmap_min(buf: &[u8]) -> io::Result<Option<u64>> {
    let tree = TreemapReader::new(buf)?;
    let bitmap = match tree.iter().next() {
        None => return Ok(None),
        Some(b) => b?,
    };
    if bitmap.containers() == 0 {
        return Ok(None);
    }
    let desc = bitmap.description(0)?;
    let offset = bitmap.container_offset(0)?;
    let container_data = &bitmap.bitmap_buf()[offset..];
    let prefix = bitmap.prefix() as u64;
    let container_key = desc.prefix as u64;
    let cardinality = desc.cardinality();
    let low16 = if cardinality < ARRAY_LIMIT {
        array_container_first(container_data, cardinality)?
    } else {
        bitmap_container_first(container_data)?
    };
    Ok(Some(prefix << 32 | container_key << 16 | low16 as u64))
}

fn array_container_first(data: &[u8], cardinality: usize) -> io::Result<u16> {
    if data.len() < cardinality * 2 {
        return Err(Error::other("array container too short"));
    }
    Ok(u16::from_le_bytes(data[0..2].try_into().unwrap()))
}

fn bitmap_container_first(data: &[u8]) -> io::Result<u16> {
    if data.len() < BITMAP_BYTES {
        return Err(Error::other("bitmap container too short"));
    }
    // Find the lowest set bit in the 1024-word bitmap
    for word_index in 0..BITMAP_WORDS {
        let start = word_index * WORD_BYTES;
        let word = u64::from_le_bytes(data[start..start + WORD_BYTES].try_into().unwrap());
        if word != 0 {
            return Ok((word_index * WORD_BITS + word.trailing_zeros() as usize) as u16);
        }
    }
    // All zeros — shouldn't happen for a valid bitmap container
    Err(Error::other("bitmap container has no set bits"))
}

pub(crate) fn bitmap_max(buf: &[u8]) -> io::Result<Option<u64>> {
    let tree = TreemapReader::new(buf)?;
    // Find the last prefix bucket
    let mut last_bitmap = None;
    for bitmap_result in tree.iter() {
        last_bitmap = Some(bitmap_result?);
    }
    let bitmap = match last_bitmap {
        None => return Ok(None),
        Some(b) => b,
    };
    if bitmap.containers() == 0 {
        return Ok(None);
    }
    let last_idx = bitmap.containers() - 1;
    let desc = bitmap.description(last_idx)?;
    let offset = bitmap.container_offset(last_idx)?;
    let container_data = &bitmap.bitmap_buf()[offset..];
    let prefix = bitmap.prefix() as u64;
    let container_key = desc.prefix as u64;
    let cardinality = desc.cardinality();
    let low16 = if cardinality < ARRAY_LIMIT {
        array_container_last(container_data, cardinality)?
    } else {
        bitmap_container_last(container_data)?
    };
    Ok(Some(prefix << 32 | container_key << 16 | low16 as u64))
}

fn array_container_last(data: &[u8], cardinality: usize) -> io::Result<u16> {
    if data.len() < cardinality * 2 {
        return Err(Error::other("array container too short"));
    }
    let offset = (cardinality - 1) * 2;
    Ok(u16::from_le_bytes(
        data[offset..offset + 2].try_into().unwrap(),
    ))
}

fn bitmap_container_last(data: &[u8]) -> io::Result<u16> {
    if data.len() < BITMAP_BYTES {
        return Err(Error::other("bitmap container too short"));
    }
    // Find the highest set bit by scanning from the last word backwards
    for word_index in (0..BITMAP_WORDS).rev() {
        let start = word_index * WORD_BYTES;
        let word = u64::from_le_bytes(data[start..start + WORD_BYTES].try_into().unwrap());
        if word != 0 {
            let bit_index = WORD_BITS - 1 - word.leading_zeros() as usize;
            return Ok((word_index * WORD_BITS + bit_index) as u16);
        }
    }
    Err(Error::other("bitmap container has no set bits"))
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

pub struct BitmapStats {
    pub len: u64,
    pub min: Option<u64>,
    pub max: Option<u64>,
}

pub(crate) fn bitmap_stats(buf: &[u8]) -> io::Result<BitmapStats> {
    let tree = TreemapReader::new(buf)?;

    let mut total_len = 0u64;
    let mut first_bitmap = None;
    let mut last_bitmap = None;

    for bitmap_result in tree.iter() {
        let bitmap = bitmap_result?;
        for i in 0..bitmap.containers() {
            total_len += bitmap.description(i)?.cardinality() as u64;
        }
        if first_bitmap.is_none() {
            first_bitmap = Some(bitmap.clone());
        }
        last_bitmap = Some(bitmap);
    }

    if first_bitmap.is_none() || total_len == 0 {
        return Ok(BitmapStats {
            len: 0,
            min: None,
            max: None,
        });
    }

    // Compute min from the first container of the first prefix bucket
    let first = first_bitmap.unwrap();
    let first_desc = first.description(0)?;
    let first_offset = first.container_offset(0)?;
    let first_container_data = &first.bitmap_buf()[first_offset..];
    let first_prefix = first.prefix() as u64;
    let first_container_key = first_desc.prefix as u64;
    let first_cardinality = first_desc.cardinality();
    let first_low16 = if first_cardinality < ARRAY_LIMIT {
        array_container_first(first_container_data, first_cardinality)?
    } else {
        bitmap_container_first(first_container_data)?
    };
    let min_val = first_prefix << 32 | first_container_key << 16 | first_low16 as u64;

    // Compute max from the last container of the last prefix bucket
    let last = last_bitmap.unwrap();
    let last_idx = last.containers() - 1;
    let last_desc = last.description(last_idx)?;
    let last_offset = last.container_offset(last_idx)?;
    let last_container_data = &last.bitmap_buf()[last_offset..];
    let last_prefix = last.prefix() as u64;
    let last_container_key = last_desc.prefix as u64;
    let last_cardinality = last_desc.cardinality();
    let last_low16 = if last_cardinality < ARRAY_LIMIT {
        array_container_last(last_container_data, last_cardinality)?
    } else {
        bitmap_container_last(last_container_data)?
    };
    let max_val = last_prefix << 32 | last_container_key << 16 | last_low16 as u64;

    Ok(BitmapStats {
        len: total_len,
        min: Some(min_val),
        max: Some(max_val),
    })
}

pub(crate) fn bitmap_has_any(lhs: &[u8], rhs: &[u8]) -> io::Result<bool> {
    use std::cmp::Ordering::*;
    let lhs_tree = TreemapReader::new(lhs)?;
    let rhs_tree = TreemapReader::new(rhs)?;

    let mut lhs_iter = lhs_tree.iter();
    let mut rhs_iter = rhs_tree.iter();

    let mut lhs_curr = lhs_iter.next().transpose()?;
    let mut rhs_curr = rhs_iter.next().transpose()?;

    while let (Some(lhs_bitmap), Some(rhs_bitmap)) = (lhs_curr.as_ref(), rhs_curr.as_ref()) {
        match lhs_bitmap.prefix().cmp(&rhs_bitmap.prefix()) {
            Less => {
                lhs_curr = lhs_iter.next().transpose()?;
            }
            Greater => {
                rhs_curr = rhs_iter.next().transpose()?;
            }
            Equal => {
                if containers_has_any(lhs_bitmap, rhs_bitmap)? {
                    return Ok(true);
                }
                lhs_curr = lhs_iter.next().transpose()?;
                rhs_curr = rhs_iter.next().transpose()?;
            }
        }
    }

    Ok(false)
}

fn containers_has_any(lhs: &BitmapReader<'_>, rhs: &BitmapReader<'_>) -> io::Result<bool> {
    let lhs_count = lhs.containers();
    let rhs_count = rhs.containers();
    let mut i = 0;
    let mut j = 0;

    while i < lhs_count && j < rhs_count {
        let lhs_desc = lhs.description(i)?;
        let rhs_desc = rhs.description(j)?;
        match lhs_desc.prefix.cmp(&rhs_desc.prefix) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                let lhs_offset = lhs.container_offset(i)?;
                let rhs_offset = rhs.container_offset(j)?;
                let lhs_data = &lhs.bitmap_buf()[lhs_offset..];
                let rhs_data = &rhs.bitmap_buf()[rhs_offset..];
                let lhs_cardinality = lhs_desc.cardinality();
                let rhs_cardinality = rhs_desc.cardinality();
                if container_has_any(lhs_data, lhs_cardinality, rhs_data, rhs_cardinality)? {
                    return Ok(true);
                }
                i += 1;
                j += 1;
            }
        }
    }

    Ok(false)
}

fn container_has_any(
    lhs_data: &[u8],
    lhs_cardinality: usize,
    rhs_data: &[u8],
    rhs_cardinality: usize,
) -> io::Result<bool> {
    // Pigeonhole: two subsets of a 65536-element space with combined size > 65536 must overlap
    if lhs_cardinality + rhs_cardinality > 65536 {
        return Ok(true);
    }

    let lhs_is_array = lhs_cardinality < ARRAY_LIMIT;
    let rhs_is_array = rhs_cardinality < ARRAY_LIMIT;

    if lhs_is_array && rhs_is_array {
        array_container_has_any(lhs_data, lhs_cardinality, rhs_data, rhs_cardinality)
    } else if !lhs_is_array && !rhs_is_array {
        bitmap_container_has_any(lhs_data, rhs_data)
    } else if lhs_is_array {
        array_bitmap_container_has_any(lhs_data, lhs_cardinality, rhs_data)
    } else {
        array_bitmap_container_has_any(rhs_data, rhs_cardinality, lhs_data)
    }
}

fn array_container_has_any(
    lhs_data: &[u8],
    lhs_cardinality: usize,
    rhs_data: &[u8],
    rhs_cardinality: usize,
) -> io::Result<bool> {
    if lhs_data.len() < lhs_cardinality * 2 {
        return Err(Error::other("lhs array container too short"));
    }
    if rhs_data.len() < rhs_cardinality * 2 {
        return Err(Error::other("rhs array container too short"));
    }

    let mut i = 0;
    let mut j = 0;
    while i < lhs_cardinality && j < rhs_cardinality {
        let lv = u16::from_le_bytes(lhs_data[i * 2..i * 2 + 2].try_into().unwrap());
        let rv = u16::from_le_bytes(rhs_data[j * 2..j * 2 + 2].try_into().unwrap());
        if lv < rv {
            i += 1;
        } else if rv < lv {
            j += 1;
        } else {
            return Ok(true);
        }
    }
    Ok(false)
}

fn bitmap_container_has_any(lhs_data: &[u8], rhs_data: &[u8]) -> io::Result<bool> {
    if lhs_data.len() < BITMAP_BYTES {
        return Err(Error::other("lhs bitmap container too short"));
    }
    if rhs_data.len() < BITMAP_BYTES {
        return Err(Error::other("rhs bitmap container too short"));
    }

    for word_index in 0..BITMAP_WORDS {
        let start = word_index * WORD_BYTES;
        let lhs_word = u64::from_le_bytes(lhs_data[start..start + WORD_BYTES].try_into().unwrap());
        let rhs_word = u64::from_le_bytes(rhs_data[start..start + WORD_BYTES].try_into().unwrap());
        if lhs_word & rhs_word != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn array_bitmap_container_has_any(
    array_data: &[u8],
    array_cardinality: usize,
    bitmap_data: &[u8],
) -> io::Result<bool> {
    if array_data.len() < array_cardinality * 2 {
        return Err(Error::other("array container too short"));
    }
    if bitmap_data.len() < BITMAP_BYTES {
        return Err(Error::other("bitmap container too short"));
    }

    for i in 0..array_cardinality {
        let value = u16::from_le_bytes(array_data[i * 2..i * 2 + 2].try_into().unwrap());
        let word_index = value as usize / WORD_BITS;
        let bit_index = value as usize % WORD_BITS;
        let start = word_index * WORD_BYTES;
        let word = u64::from_le_bytes(bitmap_data[start..start + WORD_BYTES].try_into().unwrap());
        if word & (1 << bit_index) != 0 {
            return Ok(true);
        }
    }
    Ok(false)
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
}

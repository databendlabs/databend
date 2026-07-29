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
use std::ops::ControlFlow;

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
const CONTAINER_MAX: usize = WORD_BITS * BITMAP_WORDS;

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

        let size = 4
            + last_offset as usize
            + if last_cardinality <= ARRAY_LIMIT {
                2 * last_cardinality
            } else {
                BITMAP_BYTES
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

    pub fn container(&self, index: usize) -> io::Result<ContainerReader<'_>> {
        let desc = self.description(index)?;
        let offset = self.container_offset(index)?;
        let cardinality = desc.cardinality();
        let data = &self.bitmap_buf()[offset..];

        // Validate container data length
        let required_len = if cardinality <= ARRAY_LIMIT {
            cardinality * 2
        } else {
            BITMAP_BYTES
        };
        if data.len() < required_len {
            return Err(Error::other("container data too short"));
        }

        Ok(ContainerReader {
            key: desc.prefix,
            cardinality,
            is_array: cardinality <= ARRAY_LIMIT,
            data,
        })
    }

    pub fn find_container(&self, key: u16) -> io::Result<Option<ContainerReader<'_>>> {
        let mut lo = 0;
        let mut hi = self.containers();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let desc = self.description(mid)?;
            match desc.prefix.cmp(&key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(Some(self.container(mid)?)),
            }
        }
        Ok(None)
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

pub struct ContainerReader<'a> {
    key: u16,
    cardinality: usize,
    is_array: bool,
    data: &'a [u8],
}

impl<'a> ContainerReader<'a> {
    pub fn key(&self) -> u16 {
        self.key
    }

    pub fn contains(&self, low16: u16) -> bool {
        if self.is_array {
            self.contains_array(low16)
        } else {
            self.contains_bitmap(low16)
        }
    }

    pub fn min(&self) -> Option<u16> {
        if self.is_array {
            self.array_first()
        } else {
            self.bitmap_first()
        }
    }

    pub fn max(&self) -> Option<u16> {
        if self.is_array {
            self.array_last()
        } else {
            self.bitmap_last()
        }
    }

    pub(crate) fn has_any_with(&self, other: &ContainerReader) -> bool {
        // Fast path: pigeonhole principle
        if self.cardinality + other.cardinality > CONTAINER_MAX {
            return true;
        }
        // Fast path: range non-overlapping
        if let (Some(self_max), Some(other_min)) = (self.max(), other.min())
            && self_max < other_min
        {
            return false;
        }
        if let (Some(self_min), Some(other_max)) = (self.min(), other.max())
            && self_min > other_max
        {
            return false;
        }
        // Type dispatch
        if self.is_array && other.is_array {
            self.array_has_any_array(other)
        } else if !self.is_array && !other.is_array {
            self.bitmap_has_any_bitmap(other)
        } else if self.is_array {
            self.array_has_any_bitmap(other)
        } else {
            other.array_has_any_bitmap(self)
        }
    }

    pub(crate) fn has_all_with(&self, other: &ContainerReader) -> bool {
        // Fast path: cardinality check
        if self.cardinality < other.cardinality {
            return false;
        }
        // Fast path: full bitmap
        if self.cardinality == CONTAINER_MAX {
            return true;
        }
        // Fast path: type check (array capacity < bitmap)
        if self.is_array && !other.is_array {
            return false;
        }
        // Fast path: range non-covering
        if let (Some(self_min), Some(other_min)) = (self.min(), other.min())
            && self_min > other_min
        {
            return false;
        }
        if let (Some(self_max), Some(other_max)) = (self.max(), other.max())
            && self_max < other_max
        {
            return false;
        }
        // Type dispatch
        if self.is_array && other.is_array {
            self.array_has_all_array(other)
        } else if !self.is_array && !other.is_array {
            self.bitmap_has_all_bitmap(other)
        } else {
            // self is bitmap, other is array
            self.bitmap_has_all_array(other)
        }
    }

    fn contains_array(&self, low16: u16) -> bool {
        let mut lo = 0;
        let mut hi = self.cardinality;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let v = u16::from_le_bytes(self.data[mid * 2..mid * 2 + 2].try_into().unwrap());
            match v.cmp(&low16) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return true,
            }
        }
        false
    }

    fn contains_bitmap(&self, low16: u16) -> bool {
        let word_index = low16 as usize / WORD_BITS;
        let bit_index = low16 as usize % WORD_BITS;
        let start = word_index * WORD_BYTES;
        let word = u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
        word & (1 << bit_index) != 0
    }

    fn array_first(&self) -> Option<u16> {
        if self.cardinality == 0 {
            return None;
        }
        Some(u16::from_le_bytes(self.data[0..2].try_into().unwrap()))
    }

    fn bitmap_first(&self) -> Option<u16> {
        for word_index in 0..BITMAP_WORDS {
            let start = word_index * WORD_BYTES;
            let word = u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
            if word != 0 {
                return Some((word_index * WORD_BITS + word.trailing_zeros() as usize) as u16);
            }
        }
        None
    }

    fn array_last(&self) -> Option<u16> {
        if self.cardinality == 0 {
            return None;
        }
        let offset = (self.cardinality - 1) * 2;
        Some(u16::from_le_bytes(
            self.data[offset..offset + 2].try_into().unwrap(),
        ))
    }

    fn bitmap_last(&self) -> Option<u16> {
        for word_index in (0..BITMAP_WORDS).rev() {
            let start = word_index * WORD_BYTES;
            let word = u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
            if word != 0 {
                let bit_index = WORD_BITS - 1 - word.leading_zeros() as usize;
                return Some((word_index * WORD_BITS + bit_index) as u16);
            }
        }
        None
    }

    fn array_has_any_array(&self, other: &ContainerReader) -> bool {
        let mut i = 0;
        let mut j = 0;
        while i < self.cardinality && j < other.cardinality {
            let lv = u16::from_le_bytes(self.data[i * 2..i * 2 + 2].try_into().unwrap());
            let rv = u16::from_le_bytes(other.data[j * 2..j * 2 + 2].try_into().unwrap());
            if lv < rv {
                i += 1;
            } else if rv < lv {
                j += 1;
            } else {
                return true;
            }
        }
        false
    }

    fn bitmap_has_any_bitmap(&self, other: &ContainerReader) -> bool {
        for word_index in 0..BITMAP_WORDS {
            let start = word_index * WORD_BYTES;
            let lhs_word =
                u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
            let rhs_word =
                u64::from_le_bytes(other.data[start..start + WORD_BYTES].try_into().unwrap());
            if lhs_word & rhs_word != 0 {
                return true;
            }
        }
        false
    }

    fn array_has_any_bitmap(&self, other: &ContainerReader) -> bool {
        for i in 0..self.cardinality {
            let value = u16::from_le_bytes(self.data[i * 2..i * 2 + 2].try_into().unwrap());
            let word_index = value as usize / WORD_BITS;
            let bit_index = value as usize % WORD_BITS;
            let start = word_index * WORD_BYTES;
            let word =
                u64::from_le_bytes(other.data[start..start + WORD_BYTES].try_into().unwrap());
            if word & (1 << bit_index) != 0 {
                return true;
            }
        }
        false
    }

    fn array_has_all_array(&self, other: &ContainerReader) -> bool {
        let mut i = 0;
        let mut j = 0;
        while j < other.cardinality {
            if i >= self.cardinality {
                return false;
            }
            let lv = u16::from_le_bytes(self.data[i * 2..i * 2 + 2].try_into().unwrap());
            let rv = u16::from_le_bytes(other.data[j * 2..j * 2 + 2].try_into().unwrap());
            if lv < rv {
                i += 1;
            } else if lv == rv {
                i += 1;
                j += 1;
            } else {
                return false;
            }
        }
        true
    }

    fn bitmap_has_all_bitmap(&self, other: &ContainerReader) -> bool {
        for word_index in 0..BITMAP_WORDS {
            let start = word_index * WORD_BYTES;
            let lhs_word =
                u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
            let rhs_word =
                u64::from_le_bytes(other.data[start..start + WORD_BYTES].try_into().unwrap());
            // Check if all bits set in rhs are also set in lhs
            if rhs_word & !lhs_word != 0 {
                return false;
            }
        }
        true
    }

    fn bitmap_has_all_array(&self, other: &ContainerReader) -> bool {
        for i in 0..other.cardinality {
            let value = u16::from_le_bytes(other.data[i * 2..i * 2 + 2].try_into().unwrap());
            let word_index = value as usize / WORD_BITS;
            let bit_index = value as usize % WORD_BITS;
            let start = word_index * WORD_BYTES;
            let word = u64::from_le_bytes(self.data[start..start + WORD_BYTES].try_into().unwrap());
            if word & (1 << bit_index) == 0 {
                return false;
            }
        }
        true
    }
}

/// Handler trait for zero-copy container-pair traversal.
///
/// RoaringTreemap has two levels: prefix buckets (u32) and containers (u16 key).
/// `ContainerVisitor` walks both levels in sorted order and dispatches to the
/// handler at four kinds of events:
///
/// - `handle_lhs_only`: a container exists in lhs but not in rhs at the same key.
/// - `handle_rhs_only`: a container exists in rhs but not in lhs at the same key.
/// - `handle_matched`: both sides have a container at the same key.
/// - `handle_none`: both bitmaps are empty.
///
/// The `remaining` parameters signal whether the other side has more data
/// beyond the current prefix bucket or container key. This lets handlers
/// decide early termination (e.g., `has_any` returns false when both sides
/// are exhausted with no match).
///
/// Return `ControlFlow::Break(output)` to stop traversal immediately,
/// or `ControlFlow::Continue(())` to keep walking.
trait ContainerHandler {
    type Output;

    fn handle_lhs_only(
        &mut self,
        container: &ContainerReader,
        rhs_remaining: bool,
    ) -> ControlFlow<Self::Output>;
    fn handle_rhs_only(
        &mut self,
        container: &ContainerReader,
        lhs_remaining: bool,
    ) -> ControlFlow<Self::Output>;
    fn handle_matched(
        &mut self,
        lhs: &ContainerReader,
        rhs: &ContainerReader,
        lhs_remaining: bool,
        rhs_remaining: bool,
    ) -> ControlFlow<Self::Output>;
    fn handle_none(&mut self) -> Self::Output;
}

struct ContainerVisitor<'a, H> {
    lhs_buf: &'a [u8],
    rhs_buf: &'a [u8],
    handler: H,
}

impl<'a, H> ContainerVisitor<'a, H> {
    fn new(lhs_buf: &'a [u8], rhs_buf: &'a [u8], handler: H) -> Self {
        Self {
            lhs_buf,
            rhs_buf,
            handler,
        }
    }
}

impl<H: ContainerHandler> ContainerVisitor<'_, H> {
    fn visit(mut self) -> io::Result<H::Output> {
        use std::cmp::Ordering::*;

        let lhs_tree = TreemapReader::new(self.lhs_buf)?;
        let rhs_tree = TreemapReader::new(self.rhs_buf)?;

        let mut lhs_iter = lhs_tree.iter();
        let mut rhs_iter = rhs_tree.iter();
        let mut lhs_cur = lhs_iter.next().transpose()?;
        let mut rhs_cur = rhs_iter.next().transpose()?;

        // Both empty
        if lhs_cur.is_none() && rhs_cur.is_none() {
            return Ok(self.handler.handle_none());
        }

        // Treemap-level traversal
        while let (Some(lhs_bitmap), Some(rhs_bitmap)) = (lhs_cur.as_ref(), rhs_cur.as_ref()) {
            match lhs_bitmap.prefix().cmp(&rhs_bitmap.prefix()) {
                Less => {
                    if let ControlFlow::Break(r) =
                        self.visit_one_side_containers(lhs_bitmap, true, true)?
                    {
                        return Ok(r);
                    }
                    lhs_cur = lhs_iter.next().transpose()?;
                }
                Greater => {
                    if let ControlFlow::Break(r) =
                        self.visit_one_side_containers(rhs_bitmap, false, true)?
                    {
                        return Ok(r);
                    }
                    rhs_cur = rhs_iter.next().transpose()?;
                }
                Equal => {
                    let next_lhs = lhs_iter.next().transpose()?;
                    let next_rhs = rhs_iter.next().transpose()?;
                    let lhs_has_more_prefixes = next_lhs.is_some();
                    let rhs_has_more_prefixes = next_rhs.is_some();

                    if let ControlFlow::Break(r) = self.visit_both_sides_containers(
                        lhs_bitmap,
                        rhs_bitmap,
                        lhs_has_more_prefixes,
                        rhs_has_more_prefixes,
                    )? {
                        return Ok(r);
                    }

                    lhs_cur = next_lhs;
                    rhs_cur = next_rhs;
                }
            }
        }

        // Handle remaining containers if any
        if let ControlFlow::Break(r) =
            self.visit_one_side_remaining_containers(lhs_cur.as_ref(), &mut lhs_iter, true)?
        {
            return Ok(r);
        }
        if let ControlFlow::Break(r) =
            self.visit_one_side_remaining_containers(rhs_cur.as_ref(), &mut rhs_iter, false)?
        {
            return Ok(r);
        }

        Ok(self.handler.handle_none())
    }

    fn visit_one_side_containers(
        &mut self,
        bitmap: &BitmapReader<'_>,
        is_lhs: bool,
        other_remaining: bool,
    ) -> io::Result<ControlFlow<H::Output>> {
        for i in 0..bitmap.containers() {
            let container = bitmap.container(i)?;
            if let ControlFlow::Break(r) =
                self.visit_one_side_container(&container, is_lhs, other_remaining)
            {
                return Ok(ControlFlow::Break(r));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn visit_both_sides_containers(
        &mut self,
        lhs_bitmap: &BitmapReader<'_>,
        rhs_bitmap: &BitmapReader<'_>,
        lhs_has_more_prefixes: bool,
        rhs_has_more_prefixes: bool,
    ) -> io::Result<ControlFlow<H::Output>> {
        use std::cmp::Ordering::*;

        let mut i = 0;
        let mut j = 0;
        let lhs_count = lhs_bitmap.containers();
        let rhs_count = rhs_bitmap.containers();
        while i < lhs_count && j < rhs_count {
            let lhs_container = lhs_bitmap.container(i)?;
            let rhs_container = rhs_bitmap.container(j)?;
            match lhs_container.key().cmp(&rhs_container.key()) {
                Less => {
                    if let ControlFlow::Break(r) = self.visit_one_side_container(
                        &lhs_container,
                        true, // left
                        true, // rhs has more
                    ) {
                        return Ok(ControlFlow::Break(r));
                    }
                    i += 1;
                }
                Greater => {
                    if let ControlFlow::Break(r) = self.visit_one_side_container(
                        &rhs_container,
                        false, // right
                        true,  // lhs has more
                    ) {
                        return Ok(ControlFlow::Break(r));
                    }
                    j += 1;
                }
                Equal => {
                    let lhs_remaining = (i + 1 < lhs_count) || lhs_has_more_prefixes;
                    let rhs_remaining = (j + 1 < rhs_count) || rhs_has_more_prefixes;
                    if let ControlFlow::Break(r) = self.handler.handle_matched(
                        &lhs_container,
                        &rhs_container,
                        lhs_remaining,
                        rhs_remaining,
                    ) {
                        return Ok(ControlFlow::Break(r));
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        while i < lhs_count {
            let container = lhs_bitmap.container(i)?;
            if let ControlFlow::Break(r) =
                self.visit_one_side_container(&container, true, rhs_has_more_prefixes)
            {
                return Ok(ControlFlow::Break(r));
            }
            i += 1;
        }
        while j < rhs_count {
            let container = rhs_bitmap.container(j)?;
            if let ControlFlow::Break(r) =
                self.visit_one_side_container(&container, false, lhs_has_more_prefixes)
            {
                return Ok(ControlFlow::Break(r));
            }
            j += 1;
        }
        Ok(ControlFlow::Continue(()))
    }

    fn visit_one_side_remaining_containers(
        &mut self,
        cur: Option<&BitmapReader<'_>>,
        remaining: &mut TreeMapIter<'_>,
        is_lhs: bool,
    ) -> io::Result<ControlFlow<H::Output>> {
        if let Some(bitmap) = cur
            && let ControlFlow::Break(r) = self.visit_one_side_containers(bitmap, is_lhs, false)?
        {
            return Ok(ControlFlow::Break(r));
        }
        while let Some(bitmap) = remaining.next().transpose()? {
            if let ControlFlow::Break(r) = self.visit_one_side_containers(&bitmap, is_lhs, false)? {
                return Ok(ControlFlow::Break(r));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn visit_one_side_container(
        &mut self,
        container: &ContainerReader,
        is_lhs: bool,
        other_remaining: bool,
    ) -> ControlFlow<H::Output> {
        if is_lhs {
            self.handler.handle_lhs_only(container, other_remaining)
        } else {
            self.handler.handle_rhs_only(container, other_remaining)
        }
    }
}

struct HasAnyHandler;

impl ContainerHandler for HasAnyHandler {
    type Output = bool;

    fn handle_lhs_only(&mut self, _: &ContainerReader, rhs_remaining: bool) -> ControlFlow<bool> {
        if !rhs_remaining {
            return ControlFlow::Break(false);
        }
        ControlFlow::Continue(())
    }

    fn handle_rhs_only(&mut self, _: &ContainerReader, lhs_remaining: bool) -> ControlFlow<bool> {
        if !lhs_remaining {
            return ControlFlow::Break(false);
        }
        ControlFlow::Continue(())
    }

    fn handle_matched(
        &mut self,
        lhs: &ContainerReader,
        rhs: &ContainerReader,
        lhs_remaining: bool,
        rhs_remaining: bool,
    ) -> ControlFlow<bool> {
        if lhs.has_any_with(rhs) {
            ControlFlow::Break(true)
        } else if !lhs_remaining || !rhs_remaining {
            ControlFlow::Break(false)
        } else {
            ControlFlow::Continue(())
        }
    }

    fn handle_none(&mut self) -> bool {
        false
    }
}

struct HasAllHandler;

impl ContainerHandler for HasAllHandler {
    type Output = bool;

    fn handle_lhs_only(&mut self, _: &ContainerReader, _: bool) -> ControlFlow<bool> {
        ControlFlow::Continue(())
    }

    fn handle_rhs_only(&mut self, _: &ContainerReader, _: bool) -> ControlFlow<bool> {
        ControlFlow::Break(false)
    }

    fn handle_matched(
        &mut self,
        lhs: &ContainerReader,
        rhs: &ContainerReader,
        lhs_remaining: bool,
        rhs_remaining: bool,
    ) -> ControlFlow<bool> {
        if !lhs.has_all_with(rhs) || (!lhs_remaining && rhs_remaining) {
            ControlFlow::Break(false)
        } else {
            ControlFlow::Continue(())
        }
    }

    fn handle_none(&mut self) -> bool {
        true
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

pub(crate) fn bitmap_len_above(buf: &[u8], threshold: usize) -> io::Result<bool> {
    let tree = TreemapReader::new(buf)?;
    let mut sum = 0;
    for bitmap in tree.iter() {
        let bitmap = bitmap?;
        for i in 0..bitmap.containers() {
            sum += bitmap.description(i)?.cardinality();
            if sum > threshold {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn bitmap_contains(buf: &[u8], value: u64) -> io::Result<bool> {
    let prefix = (value >> 32) as u32;
    let container_key = ((value >> 16) & 0xFFFF) as u16;
    let low16 = (value & 0xFFFF) as u16;

    let tree = TreemapReader::new(buf)?;
    for bitmap_result in tree.iter() {
        let bitmap = bitmap_result?;
        if bitmap.prefix() < prefix {
            continue;
        }
        if bitmap.prefix() > prefix {
            return Ok(false);
        }
        return Ok(bitmap
            .find_container(container_key)?
            .is_some_and(|c| c.contains(low16)));
    }
    Ok(false)
}

pub(crate) fn bitmap_min(buf: &[u8]) -> io::Result<Option<u64>> {
    let tree = TreemapReader::new(buf)?;
    for bitmap_result in tree.iter() {
        let bitmap = bitmap_result?;
        if bitmap.containers() == 0 {
            continue;
        }
        let container = bitmap.container(0)?;
        if let Some(low16) = container.min() {
            let prefix = bitmap.prefix() as u64;
            return Ok(Some(
                (prefix << 32) | ((container.key() as u64) << 16) | low16 as u64,
            ));
        }
    }
    Ok(None)
}

pub(crate) fn bitmap_max(buf: &[u8]) -> io::Result<Option<u64>> {
    let tree = TreemapReader::new(buf)?;
    let mut last_result = None;
    for bitmap_result in tree.iter() {
        let bitmap = bitmap_result?;
        let last_idx = bitmap.containers() - 1;
        let container = bitmap.container(last_idx)?;
        if let Some(low16) = container.max() {
            let prefix = bitmap.prefix() as u64;
            last_result = Some((prefix << 32) | ((container.key() as u64) << 16) | low16 as u64);
        }
    }
    Ok(last_result)
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

pub(crate) fn bitmap_has_any(lhs: &[u8], rhs: &[u8]) -> io::Result<bool> {
    ContainerVisitor::new(lhs, rhs, HasAnyHandler).visit()
}

pub(crate) fn bitmap_has_all(lhs: &[u8], rhs: &[u8]) -> io::Result<bool> {
    ContainerVisitor::new(lhs, rhs, HasAllHandler).visit()
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

    // Tests for minimum deserialize bitmap functions.
    //
    // Cover public functions by comparing against RoaringTreemap:
    //  - [x] bitmap_contains
    //  - [x] bitmap_min
    //  - [x] bitmap_max
    //  - [x] bitmap_has_any
    //  - [x] bitmap_has_all
    //
    // Fixtures:
    //  - `for_each_fixture` for single-bitmap tests (contains, min, max)
    //  - `for_each_fixture_pair` for two-bitmap tests (has_any, has_all)
    fn make_buf(tree: &RoaringTreemap) -> Vec<u8> {
        let mut buf = Vec::new();
        tree.serialize_into(&mut buf).unwrap();
        buf
    }

    fn for_each_fixture<F>(mut f: F)
    where F: FnMut(&str, &[u8], &RoaringTreemap, u64) {
        let fixtures: Vec<(&str, RoaringTreemap, u64)> = vec![
            ("random", create_bitmap(123), u64::MAX),
            (
                "sparse",
                RoaringTreemap::from_iter([0u64, 65535, 65536, (1u64 << 32) + 500]),
                1,
            ),
            // p0 keys 0,1,2 (bitmap) multi-key bitmap
            (
                "key: multi-array",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x10000u64..0x20000).step_by(7))
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                0x40000,
            ),
            // p0 keys 0,1 (bitmap) multi-key bitmap
            (
                "key: multi-bitmap",
                (0u64..50000).chain(0x10000u64..0x10000 + 50000).collect(),
                0x40000,
            ),
            // p0, p1 (bitmap) multi-prefix
            (
                "prefix: multi",
                (0u64..0x10000)
                    .step_by(7)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(7))
                    .collect(),
                (2u64 << 32),
            ),
        ];
        for (name, tree, miss) in fixtures {
            let buf = make_buf(&tree);
            f(name, &buf, &tree, miss);
        }
        let empty_tree = RoaringTreemap::new();
        let empty_buf = make_buf(&empty_tree);
        f("empty", &empty_buf, &empty_tree, 0);
    }

    fn for_each_fixture_pair<F>(mut f: F)
    where F: FnMut(&str, &[u8], &[u8], &RoaringTreemap, &RoaringTreemap) {
        let scenarios: Vec<(&str, RoaringTreemap, RoaringTreemap)> = vec![
            // lhs: p0 key 0 (bitmap) | rhs: p0 key 0 (bitmap) overlap
            (
                "basic: overlap",
                (0..50000u64).collect(),
                (30000..80000u64).collect(),
            ),
            // lhs: p0 key 0 (bitmap) | rhs: p0 keys 1,2 (bitmap) disjoint
            (
                "basic: disjoint",
                (0..50000u64).collect(),
                (100000..150000u64).collect(),
            ),
            // lhs: p0 key 0 (bitmap) | rhs: p0 key 0 (array) superset
            (
                "basic: superset",
                (0..50000u64).collect(),
                (0..100u64).collect(),
            ),
            // lhs: p0 key 0 (bitmap) | rhs: p0 keys 0,1 (bitmap) not superset
            (
                "basic: not superset",
                (0..50000u64).collect(),
                (40000..90000u64).collect(),
            ),
            // lhs: p0 key 0 (bitmap) | rhs: p0 key 0 (bitmap) self
            (
                "basic: self",
                (0..50000u64).collect(),
                (0..50000u64).collect(),
            ),
            // lhs: p0 key 2 (bitmap) | rhs: p0 keys 1,2 (bitmap) rhs-only key before lhs
            (
                "key: rhs-only before lhs",
                (0x20000u64..0x30000).step_by(3).collect(),
                (0x10000u64..0x30000).step_by(2).collect(),
            ),
            // lhs: p0 keys 0,1,2 (bitmap) | rhs: p0 keys 1,2 (bitmap) lhs-only key before rhs
            (
                "key: lhs-only before rhs",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x10000u64..0x20000).step_by(7))
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                (0x10000u64..0x20000)
                    .step_by(5)
                    .chain((0x20000u64..0x30000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 key 1 (bitmap) | rhs: p0 keys 1,2 (bitmap) first key shared, rhs has more
            (
                "key: lhs first shared",
                (0x10000u64..0x20000).step_by(3).collect(),
                (0x10000u64..0x30000).step_by(2).collect(),
            ),
            // lhs: p0 keys 1,2 (bitmap) | rhs: p0 key 1 (bitmap) first key shared, lhs has more
            (
                "key: rhs first shared",
                (0x10000u64..0x30000).step_by(2).collect(),
                (0x10000u64..0x20000).step_by(3).collect(),
            ),
            // lhs: p0 keys 0,2 (bitmap) | rhs: p0 keys 0,1,2 (bitmap) rhs-only key between lhs keys
            (
                "key: lhs skips middle",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain((0x10000u64..0x20000).step_by(5))
                    .chain((0x20000u64..0x30000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 keys 0,1,2 (bitmap) | rhs: p0 keys 0,2 (bitmap) lhs-only key between rhs keys
            (
                "key: rhs skips middle",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x10000u64..0x20000).step_by(7))
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain((0x20000u64..0x30000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 keys 0,1,2 (bitmap) | rhs: p0 keys 0,1 (bitmap) lhs-only key after rhs last
            (
                "key: lhs-only after rhs",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x10000u64..0x20000).step_by(7))
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain((0x10000u64..0x20000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 keys 0,1 (bitmap) | rhs: p0 keys 0,1,2 (bitmap) rhs-only key after lhs last
            (
                "key: rhs-only after lhs",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x10000u64..0x20000).step_by(7))
                    .collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain((0x10000u64..0x20000).step_by(5))
                    .chain((0x20000u64..0x30000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 keys 0,2 (bitmap) | rhs: p0 keys 1,3 (bitmap) no matching keys
            (
                "key: interleaved",
                (0u64..0x10000)
                    .step_by(7)
                    .chain((0x20000u64..0x30000).step_by(7))
                    .collect(),
                (0x10000u64..0x20000)
                    .step_by(5)
                    .chain((0x30000u64..0x40000).step_by(5))
                    .collect(),
            ),
            // lhs: p0 (bitmap) | rhs: p0, p1 (bitmap) rhs-only prefix
            (
                "prefix: rhs-only",
                (0u64..0x10000).step_by(7).collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(5))
                    .collect(),
            ),
            // lhs: p0, p1 (bitmap) | rhs: p0 (bitmap) lhs-only prefix
            (
                "prefix: lhs-only",
                (0u64..0x10000)
                    .step_by(7)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(7))
                    .collect(),
                (0u64..0x10000).step_by(5).collect(),
            ),
            // lhs: p0 (bitmap) | rhs: p1 (bitmap) disjoint prefixes
            (
                "prefix: disjoint",
                (0u64..0x10000).step_by(7).collect(),
                ((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(5).collect(),
            ),
            // lhs: p0 key 0 (array) | rhs: p0 key 0 (bitmap) matched array+bitmap
            (
                "store: array+bitmap",
                (0u64..1000).collect(),
                (0u64..50000).collect(),
            ),
            // lhs: p0 key 0 (bitmap) | rhs: p0 key 0 (array) matched bitmap+array
            (
                "store: bitmap+array",
                (0u64..50000).collect(),
                (0u64..1000).collect(),
            ),
            // lhs: p0 key 0 (array), key 1 (bitmap) | rhs: p0 key 0 (bitmap), key 1 (array) mixed store across keys
            (
                "store: mixed",
                (0u64..1000).chain(0x10000u64..0x10000 + 50000).collect(),
                (0u64..50000).chain(0x10000u64..0x10000 + 1000).collect(),
            ),
        ];
        for (name, lhs_tree, rhs_tree) in scenarios {
            let lhs_buf = make_buf(&lhs_tree);
            let rhs_buf = make_buf(&rhs_tree);
            f(name, &lhs_buf, &rhs_buf, &lhs_tree, &rhs_tree);
        }
        // Empty scenarios
        let empty_tree = RoaringTreemap::new();
        let empty_buf = make_buf(&empty_tree);
        let non_empty: RoaringTreemap = (0..50000u64).collect();
        let non_empty_buf = make_buf(&non_empty);
        f(
            "empty+non-empty",
            &empty_buf,
            &non_empty_buf,
            &empty_tree,
            &non_empty,
        );
        f(
            "non-empty+empty",
            &non_empty_buf,
            &empty_buf,
            &non_empty,
            &empty_tree,
        );
        f(
            "empty+empty",
            &empty_buf,
            &empty_buf,
            &empty_tree,
            &empty_tree,
        );
    }

    #[test]
    fn test_bitmap_contains() -> io::Result<()> {
        for_each_fixture(|name, buf, tree, miss_value| {
            // Test a known-present value (min if exists)
            if let Some(hit) = tree.min() {
                let expected = tree.contains(hit);
                let actual = bitmap_contains(buf, hit).unwrap();
                assert_eq!(
                    actual, expected,
                    "bitmap_contains hit: fixture={name}, val={hit}"
                );
            }
            // Test a known-absent value
            let miss = miss_value;
            let expected = tree.contains(miss);
            let actual = bitmap_contains(buf, miss).unwrap();
            assert_eq!(
                actual, expected,
                "bitmap_contains miss: fixture={name}, val={miss}"
            );
        });
        Ok(())
    }

    #[test]
    fn test_bitmap_min() -> io::Result<()> {
        for_each_fixture(|name, buf, tree, _miss_value| {
            let expected = tree.min();
            let actual = bitmap_min(buf).unwrap();
            assert_eq!(actual, expected, "bitmap_min: fixture={name}");
        });
        Ok(())
    }

    #[test]
    fn test_bitmap_max() -> io::Result<()> {
        for_each_fixture(|name, buf, tree, _miss_value| {
            let expected = tree.max();
            let actual = bitmap_max(buf).unwrap();
            assert_eq!(actual, expected, "bitmap_max: fixture={name}");
        });
        Ok(())
    }

    #[test]
    fn test_bitmap_has_any() -> io::Result<()> {
        for_each_fixture_pair(|name, lhs_buf, rhs_buf, lhs_tree, rhs_tree| {
            let expected = !(lhs_tree & rhs_tree).is_empty();
            let actual = bitmap_has_any(lhs_buf, rhs_buf).unwrap();
            assert_eq!(actual, expected, "bitmap_has_any: fixture={name}");
        });
        Ok(())
    }

    #[test]
    fn test_bitmap_has_all() -> io::Result<()> {
        for_each_fixture_pair(|name, lhs_buf, rhs_buf, lhs_tree, rhs_tree| {
            let expected = lhs_tree.is_superset(rhs_tree);
            let actual = bitmap_has_all(lhs_buf, rhs_buf).unwrap();
            assert_eq!(actual, expected, "bitmap_has_all: fixture={name}");
        });
        Ok(())
    }

    #[test]
    fn test_bitmap_len_above() -> io::Result<()> {
        let bitmap = create_bitmap(123);
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf)?;

        let len = bitmap_len(&buf)?;

        // Threshold below actual length -> true
        assert!(bitmap_len_above(&buf, len - 1)?);
        // Threshold at actual length -> false
        assert!(!bitmap_len_above(&buf, len)?);
        // Threshold above actual length -> false
        assert!(!bitmap_len_above(&buf, len + 1)?);

        // Empty bitmap
        let empty = RoaringTreemap::new();
        let mut empty_buf = Vec::new();
        empty.serialize_into(&mut empty_buf)?;
        assert!(!bitmap_len_above(&empty_buf, 0)?);

        Ok(())
    }
}

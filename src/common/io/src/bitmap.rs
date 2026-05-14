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
use std::iter::FromIterator;
use std::mem;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;
use std::ptr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use roaring::RoaringTreemap;
use roaring::treemap::Iter;
use smallvec::SmallVec;

mod reader;

// https://github.com/ClickHouse/ClickHouse/blob/516a6ed6f8bd8c5f6eed3a10e9037580b2fb6152/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L914
pub const LARGE_THRESHOLD: usize = 32;
pub const HYBRID_MAGIC: [u8; 2] = *b"HB";
pub const HYBRID_VERSION: u8 = 1;
pub const HYBRID_KIND_SMALL: u8 = 0;
pub const HYBRID_KIND_LARGE: u8 = 1;
pub const HYBRID_HEADER_LEN: usize = 4;

type SmallBitmap = SmallVec<[u64; LARGE_THRESHOLD]>;

#[allow(clippy::large_enum_variant)]
pub enum BitmapRhs<'a> {
    Bitmap(HybridBitmap),
    Serialized(&'a [u8]),
}

#[derive(Clone, Copy)]
enum BitmapOp {
    And,
    Or,
    Xor,
    Sub,
}

#[allow(clippy::large_enum_variant)]
enum BitmapRhsView<'a> {
    Empty,
    Small(SmallValues<'a>),
    SerializedLarge(&'a [u8]),
    Large(RoaringTreemap),
}

#[allow(clippy::large_enum_variant)]
enum SmallValues<'a> {
    Owned(SmallBitmap),
    Serialized(&'a [u8]),
}

impl SmallValues<'_> {
    fn len(&self) -> usize {
        match self {
            SmallValues::Owned(values) => values.len(),
            SmallValues::Serialized(bytes) => bytes.len() / std::mem::size_of::<u64>(),
        }
    }

    fn into_small_bitmap(self) -> SmallBitmap {
        match self {
            SmallValues::Owned(values) => values,
            SmallValues::Serialized(bytes) => {
                let mut values =
                    SmallBitmap::with_capacity(bytes.len() / std::mem::size_of::<u64>());
                for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
                    small_insert(&mut values, read_u64_le(chunk));
                }
                values
            }
        }
    }

    fn for_each(self, mut func: impl FnMut(u64)) {
        match self {
            SmallValues::Owned(values) => {
                for value in values.iter().copied() {
                    func(value);
                }
            }
            SmallValues::Serialized(bytes) => {
                let mut values =
                    SmallBitmap::with_capacity(bytes.len() / std::mem::size_of::<u64>());
                for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
                    let value = read_u64_le(chunk);
                    if small_insert(&mut values, value) {
                        func(value);
                    }
                }
            }
        }
    }
}

/// Perf Tips:
/// - The deserialization performance of HybridBitmap significantly impacts the performance of Bitmap-related calculations.
/// - Calculations may frequently create new Bitmaps; reusing them as much as possible can effectively improve performance.
///  - do not use Box to construct HybridBitmap
#[allow(clippy::large_enum_variant)]
#[derive(Clone, PartialEq)]
pub enum HybridBitmap {
    Small(SmallBitmap),
    Large(RoaringTreemap),
}

impl Default for HybridBitmap {
    fn default() -> Self {
        HybridBitmap::Small(SmallBitmap::new())
    }
}

impl HybridBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> u64 {
        match self {
            HybridBitmap::Small(set) => set.len() as u64,
            HybridBitmap::Large(tree) => tree.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, value: u64) -> bool {
        if matches!(self, HybridBitmap::Small(_)) && self.len() >= LARGE_THRESHOLD as u64 {
            let _ = self.promote_to_tree();
        }
        match self {
            HybridBitmap::Small(set) => small_insert(set, value),
            HybridBitmap::Large(tree) => tree.insert(value),
        }
    }

    pub fn contains(&self, value: u64) -> bool {
        match self {
            HybridBitmap::Small(set) => set.binary_search(&value).is_ok(),
            HybridBitmap::Large(tree) => tree.contains(value),
        }
    }

    pub fn max(&self) -> Option<u64> {
        match self {
            HybridBitmap::Small(set) => set.last().copied(),
            HybridBitmap::Large(tree) => tree.max(),
        }
    }

    pub fn min(&self) -> Option<u64> {
        match self {
            HybridBitmap::Small(set) => set.first().copied(),
            HybridBitmap::Large(tree) => tree.min(),
        }
    }

    pub fn is_superset(&self, other: &Self) -> bool {
        match (self, other) {
            (HybridBitmap::Large(lhs), HybridBitmap::Large(rhs)) => lhs.is_superset(rhs),
            (HybridBitmap::Large(lhs), HybridBitmap::Small(rhs)) => {
                rhs.iter().all(|v| lhs.contains(*v))
            }
            (HybridBitmap::Small(lhs), HybridBitmap::Large(rhs)) => {
                if lhs.len() < rhs.len() as usize {
                    return false;
                }
                rhs.iter().all(|v| lhs.contains(&v))
            }
            (HybridBitmap::Small(lhs), HybridBitmap::Small(rhs)) => small_is_superset(lhs, rhs),
        }
    }

    pub fn intersection_len(&self, other: &Self) -> u64 {
        match (self, other) {
            (HybridBitmap::Large(lhs), HybridBitmap::Large(rhs)) => lhs.intersection_len(rhs),
            (HybridBitmap::Large(lhs), HybridBitmap::Small(rhs)) => {
                rhs.iter().filter(|v| lhs.contains(**v)).count() as u64
            }
            (HybridBitmap::Small(lhs), HybridBitmap::Large(rhs)) => {
                lhs.iter().filter(|v| rhs.contains(**v)).count() as u64
            }
            (HybridBitmap::Small(lhs), HybridBitmap::Small(rhs)) => {
                small_intersection_len(lhs, rhs)
            }
        }
    }

    pub fn serialize_into<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&HYBRID_MAGIC)?;
        writer.write_all(&[HYBRID_VERSION])?;
        match self {
            HybridBitmap::Small(set) => {
                writer.write_all(&[HYBRID_KIND_SMALL])?;
                let len = u8::try_from(set.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("hybrid bitmap small set size overflow: {}", set.len()),
                    )
                })?;
                writer.write_all(&[len])?;
                for value in set.iter() {
                    writer.write_all(&value.to_le_bytes())?;
                }
                Ok(())
            }
            HybridBitmap::Large(tree) => {
                writer.write_all(&[HYBRID_KIND_LARGE])?;
                tree.serialize_into(writer)
            }
        }
    }

    pub fn iter(&self) -> HybridBitmapIter<'_> {
        match self {
            HybridBitmap::Large(tree) => HybridBitmapIter {
                inner: HybridBitmapIterInner::Large(Box::new(tree.iter())),
            },
            HybridBitmap::Small(set) => HybridBitmapIter {
                inner: HybridBitmapIterInner::Small(set.iter()),
            },
        }
    }

    fn promote_to_tree(&mut self) -> &mut RoaringTreemap {
        if let HybridBitmap::Small(set) = self {
            let data = mem::take(set);
            let mut tree = RoaringTreemap::new();
            for value in data {
                tree.insert(value);
            }
            *self = HybridBitmap::Large(tree);
        }
        match self {
            HybridBitmap::Large(tree) => tree,
            HybridBitmap::Small(_) => unreachable!(),
        }
    }

    fn try_demote(&mut self) {
        if let HybridBitmap::Large(tree) = self
            && (tree.len() as usize) <= LARGE_THRESHOLD
        {
            let data = mem::take(tree);
            let mut set = SmallBitmap::with_capacity(data.len() as usize);
            for value in data.into_iter() {
                set.push(value);
            }
            *self = HybridBitmap::Small(set);
        }
    }
}

impl HybridBitmap {
    pub fn bitor_assign_rhs(&mut self, rhs: BitmapRhs<'_>) -> Result<()> {
        self.apply_assign(BitmapOp::Or, rhs)
    }

    pub fn bitand_assign_rhs(&mut self, rhs: BitmapRhs<'_>) -> Result<()> {
        self.apply_assign(BitmapOp::And, rhs)
    }

    pub fn bitxor_assign_rhs(&mut self, rhs: BitmapRhs<'_>) -> Result<()> {
        self.apply_assign(BitmapOp::Xor, rhs)
    }

    pub fn sub_assign_rhs(&mut self, rhs: BitmapRhs<'_>) -> Result<()> {
        self.apply_assign(BitmapOp::Sub, rhs)
    }

    fn apply_assign(&mut self, op: BitmapOp, rhs: BitmapRhs<'_>) -> Result<()> {
        if matches!(op, BitmapOp::And | BitmapOp::Sub) && self.is_empty() {
            if let BitmapRhs::Serialized(buf) = rhs {
                validate_serialized_bitmap(buf)?;
            }
            return Ok(());
        }

        let rhs = BitmapRhsView::try_from(rhs)?;
        self.apply_rhs(op, rhs)
    }

    fn apply_rhs(&mut self, op: BitmapOp, rhs: BitmapRhsView<'_>) -> Result<()> {
        match (op, rhs) {
            (BitmapOp::And, BitmapRhsView::Empty) => *self = HybridBitmap::new(),
            (_, BitmapRhsView::Empty) => {}
            (BitmapOp::And, BitmapRhsView::Small(SmallValues::Serialized(rhs))) => {
                self.bitand_assign_serialized_small(rhs)
            }
            (BitmapOp::And, BitmapRhsView::Small(rhs)) => self.bitand_assign_small(rhs),
            (BitmapOp::Or, BitmapRhsView::Small(rhs)) => self.bitor_assign_small(rhs),
            (BitmapOp::Xor, BitmapRhsView::Small(rhs)) => self.bitxor_assign_small(rhs),
            (BitmapOp::Sub, BitmapRhsView::Small(rhs)) => self.sub_assign_small(rhs),
            (BitmapOp::And, BitmapRhsView::SerializedLarge(rhs)) => {
                self.bitand_assign_serialized_large(rhs)?
            }
            (op, BitmapRhsView::SerializedLarge(rhs)) => {
                let rhs = RoaringTreemap::deserialize_unchecked_from(rhs).map_err(|e| {
                    let len = rhs.len();
                    let msg = format!("fail to decode roaring bitmap payload of size {len}: {e}");
                    ErrorCode::BadBytes(msg)
                })?;
                self.apply_rhs(op, BitmapRhsView::Large(rhs))?;
            }
            (BitmapOp::And, BitmapRhsView::Large(rhs)) => self.bitand_assign_large(rhs),
            (BitmapOp::Or, BitmapRhsView::Large(rhs)) => self.bitor_assign_large(rhs),
            (BitmapOp::Xor, BitmapRhsView::Large(rhs)) => self.bitxor_assign_large(rhs),
            (BitmapOp::Sub, BitmapRhsView::Large(rhs)) => self.sub_assign_large(rhs),
        }
        Ok(())
    }

    fn bitor_assign_small(&mut self, rhs: SmallValues<'_>) {
        match self {
            HybridBitmap::Large(lhs_tree) => rhs.for_each(|value| {
                lhs_tree.insert(value);
            }),
            HybridBitmap::Small(lhs_set) => {
                small_union(lhs_set, rhs.into_small_bitmap().as_slice());
                if lhs_set.len() >= LARGE_THRESHOLD {
                    let _ = self.promote_to_tree();
                }
            }
        }
    }

    fn bitor_assign_large(&mut self, mut rhs: RoaringTreemap) {
        match self {
            HybridBitmap::Large(lhs_tree) => lhs_tree.bitor_assign(rhs),
            HybridBitmap::Small(lhs_set) => {
                rhs.extend(lhs_set.iter().copied());
                *self = HybridBitmap::Large(rhs);
            }
        }
    }

    fn bitand_assign_small(&mut self, rhs: SmallValues<'_>) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                let mut result = SmallBitmap::with_capacity(rhs.len());
                rhs.for_each(|value| {
                    if lhs_tree.contains(value) {
                        result.push(value);
                    }
                });
                *self = HybridBitmap::Small(result);
            }
            HybridBitmap::Small(lhs_set) => match rhs {
                SmallValues::Serialized(bytes) => {
                    small_intersection_serialized_in_place(lhs_set, bytes)
                }
                values => {
                    let mut values = values.into_small_bitmap();
                    small_intersection(lhs_set, &mut values);
                }
            },
        }
    }

    fn bitand_assign_serialized_small(&mut self, rhs: &[u8]) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                let mut result = SmallBitmap::with_capacity(rhs.len() / std::mem::size_of::<u64>());
                for chunk in rhs.chunks_exact(std::mem::size_of::<u64>()) {
                    let value = read_u64_le(chunk);
                    if lhs_tree.contains(value) {
                        result.push(value);
                    }
                }
                *self = HybridBitmap::Small(result);
            }
            HybridBitmap::Small(lhs_set) => small_intersection_serialized_in_place(lhs_set, rhs),
        }
    }

    fn bitand_assign_large(&mut self, rhs: RoaringTreemap) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                lhs_tree.bitand_assign(rhs);
                self.try_demote();
            }
            HybridBitmap::Small(lhs_set) => lhs_set.retain(|value| rhs.contains(*value)),
        }
    }

    fn bitand_assign_serialized_large(&mut self, rhs: &[u8]) -> Result<()> {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                reader::intersection_with_serialized(lhs_tree, rhs)?;
            }
            HybridBitmap::Small(lhs_set) => {
                let rhs = RoaringTreemap::deserialize_unchecked_from(rhs).map_err(|e| {
                    let len = rhs.len();
                    let msg = format!("fail to decode roaring bitmap payload of size {len}: {e}");
                    ErrorCode::BadBytes(msg)
                })?;
                lhs_set.retain(|value| rhs.contains(*value));
            }
        }
        Ok(())
    }

    fn bitxor_assign_small(&mut self, rhs: SmallValues<'_>) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                let mut removed = 0;
                let mut inserted = 0;
                rhs.for_each(|value| {
                    if lhs_tree.remove(value) {
                        removed += 1;
                    } else {
                        lhs_tree.insert(value);
                        inserted += 1;
                    }
                });
                if removed > inserted {
                    self.try_demote();
                }
            }
            HybridBitmap::Small(lhs_set) => {
                small_symmetric_difference(lhs_set, rhs.into_small_bitmap().as_slice());
                if lhs_set.len() >= LARGE_THRESHOLD {
                    let _ = self.promote_to_tree();
                }
            }
        }
    }

    fn bitxor_assign_large(&mut self, mut rhs: RoaringTreemap) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                lhs_tree.bitxor_assign(rhs);
                self.try_demote();
            }
            HybridBitmap::Small(lhs_set) => {
                for value in lhs_set.iter().copied() {
                    if !rhs.remove(value) {
                        rhs.insert(value);
                    }
                }
                *self = HybridBitmap::from(rhs);
            }
        }
    }

    fn sub_assign_small(&mut self, rhs: SmallValues<'_>) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                rhs.for_each(|value| {
                    lhs_tree.remove(value);
                });
                self.try_demote();
            }
            HybridBitmap::Small(lhs_set) => {
                let result =
                    small_difference(lhs_set.as_slice(), rhs.into_small_bitmap().as_slice());
                *lhs_set = result;
            }
        }
    }

    fn sub_assign_large(&mut self, rhs: RoaringTreemap) {
        match self {
            HybridBitmap::Large(lhs_tree) => {
                lhs_tree.sub_assign(rhs);
                self.try_demote();
            }
            HybridBitmap::Small(lhs_set) => lhs_set.retain(|value| !rhs.contains(*value)),
        }
    }
}

impl From<RoaringTreemap> for HybridBitmap {
    fn from(value: RoaringTreemap) -> Self {
        if (value.len() as usize) <= LARGE_THRESHOLD {
            let mut set = SmallBitmap::with_capacity(value.len() as usize);
            for v in value.into_iter() {
                set.push(v);
            }
            HybridBitmap::Small(set)
        } else {
            HybridBitmap::Large(value)
        }
    }
}

impl FromIterator<u64> for HybridBitmap {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        let mut bitmap = HybridBitmap::new();
        for value in iter {
            bitmap.insert(value);
        }
        bitmap
    }
}

impl<'a> FromIterator<&'a u64> for HybridBitmap {
    fn from_iter<T: IntoIterator<Item = &'a u64>>(iter: T) -> Self {
        let mut bitmap = HybridBitmap::new();
        for value in iter {
            bitmap.insert(*value);
        }
        bitmap
    }
}

impl std::ops::BitOrAssign for HybridBitmap {
    fn bitor_assign(&mut self, rhs: Self) {
        self.bitor_assign_rhs(BitmapRhs::Bitmap(rhs)).unwrap();
    }
}

impl std::ops::BitOr for HybridBitmap {
    type Output = HybridBitmap;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self.bitor_assign(rhs);
        self
    }
}

impl std::ops::BitAndAssign for HybridBitmap {
    fn bitand_assign(&mut self, rhs: Self) {
        self.bitand_assign_rhs(BitmapRhs::Bitmap(rhs)).unwrap();
    }
}

impl std::ops::BitAnd for HybridBitmap {
    type Output = HybridBitmap;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self.bitand_assign(rhs);
        self
    }
}

impl std::ops::BitXorAssign for HybridBitmap {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.bitxor_assign_rhs(BitmapRhs::Bitmap(rhs)).unwrap();
    }
}

impl std::ops::BitXor for HybridBitmap {
    type Output = HybridBitmap;

    fn bitxor(mut self, rhs: Self) -> Self::Output {
        self.bitxor_assign(rhs);
        self
    }
}

impl std::ops::SubAssign for HybridBitmap {
    fn sub_assign(&mut self, rhs: Self) {
        self.sub_assign_rhs(BitmapRhs::Bitmap(rhs)).unwrap();
    }
}

impl std::ops::Sub for HybridBitmap {
    type Output = HybridBitmap;

    fn sub(mut self, rhs: Self) -> Self::Output {
        self.sub_assign(rhs);
        self
    }
}

pub struct HybridBitmapIter<'a> {
    inner: HybridBitmapIterInner<'a>,
}

enum HybridBitmapIterInner<'a> {
    Large(Box<Iter<'a>>),
    Small(std::slice::Iter<'a, u64>),
}

impl<'a> Iterator for HybridBitmapIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            HybridBitmapIterInner::Large(iter) => iter.next(),
            HybridBitmapIterInner::Small(iter) => iter.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            HybridBitmapIterInner::Large(iter) => iter.size_hint(),
            HybridBitmapIterInner::Small(iter) => iter.size_hint(),
        }
    }
}

impl<'a> IntoIterator for &'a HybridBitmap {
    type Item = u64;
    type IntoIter = HybridBitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct HybridBitmapIntoIter {
    inner: HybridBitmapIntoIterInner,
}

enum HybridBitmapIntoIterInner {
    Large(Box<<RoaringTreemap as IntoIterator>::IntoIter>),
    Small(Box<smallvec::IntoIter<[u64; LARGE_THRESHOLD]>>),
}

impl Iterator for HybridBitmapIntoIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            HybridBitmapIntoIterInner::Large(iter) => iter.next(),
            HybridBitmapIntoIterInner::Small(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            HybridBitmapIntoIterInner::Large(iter) => iter.size_hint(),
            HybridBitmapIntoIterInner::Small(iter) => iter.size_hint(),
        }
    }
}

impl IntoIterator for HybridBitmap {
    type Item = u64;
    type IntoIter = HybridBitmapIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            HybridBitmap::Large(tree) => HybridBitmapIntoIter {
                inner: HybridBitmapIntoIterInner::Large(Box::new(tree.into_iter())),
            },
            HybridBitmap::Small(set) => HybridBitmapIntoIter {
                inner: HybridBitmapIntoIterInner::Small(Box::new(set.into_iter())),
            },
        }
    }
}

impl<'a> TryFrom<BitmapRhs<'a>> for BitmapRhsView<'a> {
    type Error = ErrorCode;

    fn try_from(rhs: BitmapRhs<'a>) -> Result<Self> {
        match rhs {
            BitmapRhs::Bitmap(HybridBitmap::Small(values)) => {
                Ok(BitmapRhsView::Small(SmallValues::Owned(values)))
            }
            BitmapRhs::Bitmap(HybridBitmap::Large(tree)) => Ok(BitmapRhsView::Large(tree)),
            BitmapRhs::Serialized(buf) => parse_bitmap_rhs(buf),
        }
    }
}

impl fmt::Debug for HybridBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<u64> = self.iter().collect();
        write!(f, "HybridBitmap<{values:?}>")
    }
}

pub fn parse_bitmap(buf: &[u8]) -> Result<HybridBitmap> {
    std::str::from_utf8(buf)
        .map_err(|_| ())
        .and_then(|s| {
            let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
            let mut map = HybridBitmap::new();

            for v in s.split(',') {
                let result = v.parse::<u64>().map_err(|_| ())?;
                map.insert(result);
            }
            Ok(map)
        })
        .map_err(|_| {
            ErrorCode::BadBytes(format!(
                "Invalid Bitmap value: {:?}",
                String::from_utf8_lossy(buf)
            ))
        })
}

pub fn deserialize_bitmap(buf: &[u8]) -> Result<HybridBitmap> {
    if buf.is_empty() {
        return Ok(HybridBitmap::new());
    }

    if let Some(result) = try_decode_hybrid_bitmap(buf) {
        return result;
    }

    RoaringTreemap::deserialize_unchecked_from(buf)
        .map(HybridBitmap::from)
        .map_err(|e| {
            let len = buf.len();
            let msg = format!("fail to decode bitmap from buffer of size {len}: {e}");
            ErrorCode::BadBytes(msg)
        })
}

pub fn bitmap_len(buf: &[u8]) -> Result<u64> {
    if buf.is_empty() {
        return Ok(0);
    }

    if buf.len() > 3
        && buf[3] == HYBRID_KIND_LARGE
        && buf[..2] == HYBRID_MAGIC
        && buf[2] == HYBRID_VERSION
    {
        Ok(reader::bitmap_len(&buf[HYBRID_HEADER_LEN..])? as u64)
    } else {
        Ok(deserialize_bitmap(buf)?.len())
    }
}

fn parse_bitmap_rhs(buf: &[u8]) -> Result<BitmapRhsView<'_>> {
    if buf.is_empty() {
        return Ok(BitmapRhsView::Empty);
    }

    if buf.len() >= HYBRID_HEADER_LEN && buf[..2] == HYBRID_MAGIC && buf[2] == HYBRID_VERSION {
        let payload = &buf[HYBRID_HEADER_LEN..];
        match buf[3] {
            HYBRID_KIND_SMALL => {
                let (_, bytes) = decode_small_payload(payload)?;
                Ok(BitmapRhsView::Small(SmallValues::Serialized(bytes)))
            }
            HYBRID_KIND_LARGE => Ok(BitmapRhsView::SerializedLarge(payload)),
            kind => Err(ErrorCode::BadBytes(format!(
                "unknown hybrid bitmap kind: {kind}"
            ))),
        }
    } else {
        Ok(BitmapRhsView::try_from(BitmapRhs::Bitmap(
            deserialize_bitmap(buf)?,
        ))?)
    }
}

fn validate_serialized_bitmap(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }

    if buf.len() >= HYBRID_HEADER_LEN && buf[..2] == HYBRID_MAGIC && buf[2] == HYBRID_VERSION {
        let payload = &buf[HYBRID_HEADER_LEN..];
        match buf[3] {
            HYBRID_KIND_SMALL => {
                decode_small_payload(payload)?;
            }
            HYBRID_KIND_LARGE => {
                RoaringTreemap::deserialize_unchecked_from(payload).map_err(|e| {
                    let len = payload.len();
                    let msg = format!("fail to decode roaring bitmap payload of size {len}: {e}");
                    ErrorCode::BadBytes(msg)
                })?;
            }
            kind => {
                return Err(ErrorCode::BadBytes(format!(
                    "unknown hybrid bitmap kind: {kind}"
                )));
            }
        }
    } else {
        deserialize_bitmap(buf).map(|_| ())?;
    }

    Ok(())
}

fn try_decode_hybrid_bitmap(buf: &[u8]) -> Option<Result<HybridBitmap>> {
    if buf.len() < HYBRID_HEADER_LEN {
        return None;
    }

    if buf[0..2] != HYBRID_MAGIC {
        return None;
    }

    let version = buf[2];
    if version != HYBRID_VERSION {
        return Some(Err(ErrorCode::BadBytes(format!(
            "unsupported hybrid bitmap version: {version}"
        ))));
    }

    let kind = buf[3];
    let payload = &buf[HYBRID_HEADER_LEN..];
    match kind {
        HYBRID_KIND_SMALL => Some(decode_small_bitmap(payload)),
        HYBRID_KIND_LARGE => Some(
            RoaringTreemap::deserialize_unchecked_from(payload)
                .map(HybridBitmap::from)
                .map_err(|e| {
                    let len = payload.len();
                    let msg = format!("fail to decode roaring bitmap payload of size {len}: {e}");
                    ErrorCode::BadBytes(msg)
                }),
        ),
        _ => Some(Err(ErrorCode::BadBytes(format!(
            "unknown hybrid bitmap kind: {kind}"
        )))),
    }
}

fn decode_small_bitmap(payload: &[u8]) -> Result<HybridBitmap> {
    Ok(HybridBitmap::Small(decode_small_values(payload)?))
}

fn decode_small_values(payload: &[u8]) -> Result<SmallBitmap> {
    let (_, bytes) = decode_small_payload(payload)?;
    let mut values = SmallBitmap::with_capacity(bytes.len() / std::mem::size_of::<u64>());
    for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
        small_insert(&mut values, read_u64_le(chunk));
    }
    Ok(values)
}

fn decode_small_payload(payload: &[u8]) -> Result<(usize, &[u8])> {
    if payload.is_empty() {
        return Err(ErrorCode::BadBytes(
            "invalid hybrid bitmap payload: missing length".to_string(),
        ));
    }

    let len = payload[0] as usize;
    let bytes = &payload[1..];
    let expected = len
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| ErrorCode::BadBytes("hybrid bitmap length overflow".to_string()))?;

    if bytes.len() != expected {
        return Err(ErrorCode::BadBytes(format!(
            "invalid hybrid bitmap payload, expect {expected} value bytes but got {}",
            bytes.len()
        )));
    }

    Ok((len, bytes))
}

#[inline]
fn read_u64_le(chunk: &[u8]) -> u64 {
    let raw = unsafe { ptr::read_unaligned(chunk.as_ptr() as *const u64) };
    u64::from_le(raw)
}

fn small_insert(set: &mut SmallBitmap, value: u64) -> bool {
    match set.binary_search(&value) {
        Ok(_) => false,
        Err(pos) => {
            set.insert(pos, value);
            true
        }
    }
}

fn small_union(target: &mut SmallBitmap, other: &[u64]) {
    if other.is_empty() {
        return;
    }
    if target.is_empty() {
        target.extend_from_slice(other);
        return;
    }

    let lhs_len = target.len();
    let rhs_len = other.len();
    target.reserve(rhs_len);
    let mut write = lhs_len + rhs_len;
    target.resize(write, 0);

    let mut i = lhs_len;
    let mut j = rhs_len;

    while i > 0 && j > 0 {
        let lv = target[i - 1];
        let rv = other[j - 1];
        write -= 1;
        match lv.cmp(&rv) {
            std::cmp::Ordering::Greater => {
                target[write] = lv;
                i -= 1;
            }
            std::cmp::Ordering::Less => {
                target[write] = rv;
                j -= 1;
            }
            std::cmp::Ordering::Equal => {
                target[write] = lv;
                i -= 1;
                j -= 1;
            }
        }
    }

    while i > 0 {
        write -= 1;
        target[write] = target[i - 1];
        i -= 1;
    }

    while j > 0 {
        write -= 1;
        target[write] = other[j - 1];
        j -= 1;
    }

    if write > 0 {
        let len = target.len();
        target.copy_within(write..len, 0);
        target.truncate(len - write);
    }
}

fn small_intersection(lhs: &mut SmallBitmap, rhs: &mut SmallBitmap) {
    if lhs.is_empty() || rhs.is_empty() {
        lhs.clear();
        return;
    }

    if lhs.len() <= rhs.len() {
        let other = rhs.as_slice();
        small_intersection_in_place(lhs, other);
    } else {
        {
            let other = lhs.as_slice();
            small_intersection_in_place(rhs, other);
        }
        mem::swap(lhs, rhs);
    }
}

#[inline]
fn small_intersection_in_place(target: &mut SmallBitmap, other: &[u64]) {
    if other.is_empty() {
        target.clear();
        return;
    }

    let mut write = 0;
    let mut i = 0;
    let mut j = 0;
    let target_len = target.len();

    while i < target_len && j < other.len() {
        let lv = target[i];
        let rv = other[j];
        if lv < rv {
            i += 1;
        } else if rv < lv {
            j += 1;
        } else {
            target[write] = lv;
            write += 1;
            i += 1;
            j += 1;
        }
    }

    target.truncate(write);
}

#[inline]
fn small_intersection_serialized_in_place(target: &mut SmallBitmap, other: &[u8]) {
    if other.is_empty() {
        target.clear();
        return;
    }

    let mut write = 0;
    let mut i = 0;
    let mut j = 0;
    let target_len = target.len();
    let other_len = other.len() / std::mem::size_of::<u64>();

    while i < target_len && j < other_len {
        let lv = target[i];
        let offset = j * std::mem::size_of::<u64>();
        let rv = read_u64_le(&other[offset..offset + std::mem::size_of::<u64>()]);
        if lv < rv {
            i += 1;
        } else if rv < lv {
            j += 1;
        } else {
            target[write] = lv;
            write += 1;
            i += 1;
            j += 1;
        }
    }

    target.truncate(write);
}

fn small_difference(lhs: &[u64], rhs: &[u64]) -> SmallBitmap {
    if rhs.is_empty() {
        return SmallBitmap::from_slice(lhs);
    }

    let mut result = SmallBitmap::with_capacity(lhs.len());
    let mut i = 0;
    let mut j = 0;

    while i < lhs.len() {
        if j >= rhs.len() {
            result.extend_from_slice(&lhs[i..]);
            break;
        }

        let lv = lhs[i];
        let rv = rhs[j];
        if lv < rv {
            result.push(lv);
            i += 1;
        } else if rv < lv {
            j += 1;
        } else {
            i += 1;
            j += 1;
        }
    }
    result
}

fn small_symmetric_difference(target: &mut SmallBitmap, other: &[u64]) {
    if other.is_empty() {
        return;
    }
    if target.is_empty() {
        target.extend_from_slice(other);
        return;
    }

    let lhs_len = target.len();
    let rhs_len = other.len();
    target.reserve(rhs_len);
    let mut write = lhs_len + rhs_len;
    target.resize(write, 0);

    let mut i = lhs_len;
    let mut j = rhs_len;

    while i > 0 && j > 0 {
        let lv = target[i - 1];
        let rv = other[j - 1];
        match lv.cmp(&rv) {
            std::cmp::Ordering::Greater => {
                write -= 1;
                target[write] = lv;
                i -= 1;
            }
            std::cmp::Ordering::Less => {
                write -= 1;
                target[write] = rv;
                j -= 1;
            }
            std::cmp::Ordering::Equal => {
                i -= 1;
                j -= 1;
            }
        }
    }

    while i > 0 {
        write -= 1;
        target[write] = target[i - 1];
        i -= 1;
    }

    while j > 0 {
        write -= 1;
        target[write] = other[j - 1];
        j -= 1;
    }

    if write > 0 {
        let len = target.len();
        target.copy_within(write..len, 0);
        target.truncate(len - write);
    }
}

fn small_is_superset(lhs: &SmallBitmap, rhs: &SmallBitmap) -> bool {
    if lhs.len() < rhs.len() {
        return false;
    }
    let left = lhs.as_slice();
    let right = rhs.as_slice();
    let mut i = 0;
    let mut j = 0;

    while j < right.len() {
        while i < left.len() && left[i] < right[j] {
            i += 1;
        }
        if i == left.len() || left[i] != right[j] {
            return false;
        }
        i += 1;
        j += 1;
    }
    true
}

fn small_intersection_len(lhs: &SmallBitmap, rhs: &SmallBitmap) -> u64 {
    let left = lhs.as_slice();
    let right = rhs.as_slice();
    let mut count = 0_u64;
    let mut i = 0;
    let mut j = 0;

    while i < left.len() && j < right.len() {
        let lv = left[i];
        let rv = right[j];
        if lv < rv {
            i += 1;
        } else if rv < lv {
            j += 1;
        } else {
            count += 1;
            i += 1;
            j += 1;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use super::*;

    #[test]
    fn small_insert_keeps_sorted_unique_values() {
        let mut set: SmallBitmap = smallvec![1_u64, 3, 5];
        assert!(!small_insert(&mut set, 3));
        assert!(small_insert(&mut set, 4));
        assert!(small_insert(&mut set, 0));
        assert_eq!(set.as_slice(), &[0, 1, 3, 4, 5]);
    }

    #[test]
    fn small_union_merges_and_deduplicates() {
        let mut left: SmallBitmap = smallvec![1_u64, 3, 5];
        let right = [0_u64, 3, 4, 7];
        small_union(&mut left, &right);
        assert_eq!(left.as_slice(), &[0, 1, 3, 4, 5, 7]);
    }

    #[test]
    fn small_intersection_returns_common_values() {
        let mut lhs: SmallBitmap = smallvec![1_u64, 2, 4, 6];
        let mut rhs: SmallBitmap = smallvec![0_u64, 2, 3, 4, 5];
        small_intersection(&mut lhs, &mut rhs);
        assert_eq!(lhs.as_slice(), &[2, 4]);
    }

    #[test]
    fn small_intersection_prefers_smaller_buffer() {
        let mut lhs: SmallBitmap = smallvec![0_u64, 1, 2, 3, 4, 6];
        let mut rhs: SmallBitmap = smallvec![2_u64, 3];
        let expected_lhs = lhs.clone();

        small_intersection(&mut lhs, &mut rhs);

        assert_eq!(lhs.as_slice(), &[2, 3]);
        assert_eq!(rhs.as_slice(), expected_lhs.as_slice());
    }

    #[test]
    fn small_difference_removes_rhs_values() {
        let lhs = [1_u64, 2, 4, 6];
        let rhs = [2_u64, 3, 5];
        let result = small_difference(&lhs, &rhs);
        assert_eq!(result.as_slice(), &[1, 4, 6]);

        let result = small_difference(&lhs, &[]);
        assert_eq!(result.as_slice(), lhs);
    }

    #[test]
    fn small_symmetric_difference_handles_overlap() {
        let mut lhs: SmallBitmap = smallvec![1_u64, 2, 4];
        let rhs = [2_u64, 3, 5];
        small_symmetric_difference(&mut lhs, &rhs);
        assert_eq!(lhs.as_slice(), &[1, 3, 4, 5]);
    }

    #[test]
    fn small_is_superset_checks_lengths_and_content() {
        let lhs: SmallBitmap = smallvec![1_u64, 2, 4, 6];
        let subset: SmallBitmap = smallvec![2_u64, 4];
        let disjoint: SmallBitmap = smallvec![2_u64, 5];
        let bigger: SmallBitmap = smallvec![1_u64, 2, 4, 6, 8];

        assert!(small_is_superset(&lhs, &subset));
        assert!(!small_is_superset(&lhs, &disjoint));
        assert!(!small_is_superset(&lhs, &bigger));
    }

    #[test]
    fn small_intersection_len_counts_matches() {
        let lhs: SmallBitmap = smallvec![1_u64, 3, 4, 8, 10];
        let rhs: SmallBitmap = smallvec![0_u64, 3, 5, 8, 9];
        let empty: SmallBitmap = smallvec![];

        assert_eq!(small_intersection_len(&lhs, &rhs), 2);
        assert_eq!(small_intersection_len(&lhs, &empty), 0);
    }

    #[test]
    fn promote_and_demote_between_representations() {
        let mut bitmap = HybridBitmap::from_iter([0_u64, 1, 2]);
        assert!(matches!(bitmap, HybridBitmap::Small(_)));

        let mut roaring = RoaringTreemap::new();
        for i in 0..=(LARGE_THRESHOLD as u64) {
            roaring.insert(i);
        }
        let large = HybridBitmap::from(roaring);
        assert!(matches!(large, HybridBitmap::Large(_)));

        bitmap.bitor_assign(large);
        assert!(matches!(bitmap, HybridBitmap::Large(_)));

        let other = HybridBitmap::from_iter([0_u64, 1, 2]);
        bitmap.bitand_assign(other.clone());
        assert!(matches!(bitmap, HybridBitmap::Small(_)));
        assert_eq!(bitmap.len(), other.len());
    }

    #[test]
    fn bitand_large_with_small_demotes_to_small() {
        let mut large = HybridBitmap::from_iter(0_u64..32);
        let rhs = HybridBitmap::from_iter([1_u64, 5, 7, 40]);
        large.bitand_assign(rhs);

        match large {
            HybridBitmap::Small(set) => {
                assert_eq!(set.as_slice(), &[1, 5, 7]);
            }
            _ => panic!("expected small hybrid bitmap after intersection"),
        }
    }

    #[test]
    fn bitand_small_with_large_stays_small() {
        let mut small = HybridBitmap::from_iter([1_u64, 5, 7, 40]);
        let large = HybridBitmap::from_iter(0_u64..64);
        small.bitand_assign(large);

        match small {
            HybridBitmap::Small(set) => assert_eq!(set.as_slice(), &[1, 5, 7, 40]),
            _ => panic!("expected small hybrid bitmap after intersection"),
        }
    }

    #[test]
    fn bitor_small_with_large_uses_large_rhs() {
        let mut small = HybridBitmap::from_iter([100_u64, 101]);
        let large = HybridBitmap::from_iter(0_u64..64);
        small.bitor_assign(large);

        assert!(matches!(small, HybridBitmap::Large(_)));
        assert!(small.contains(1));
        assert!(small.contains(100));
        assert_eq!(small.len(), 66);
    }

    #[test]
    fn bitxor_small_with_large_toggles_values() {
        let mut small = HybridBitmap::from_iter([1_u64, 5, 100]);
        let large = HybridBitmap::from_iter(0_u64..64);
        small.bitxor_assign(large);

        assert!(!small.contains(1));
        assert!(!small.contains(5));
        assert!(small.contains(2));
        assert!(small.contains(100));
        assert_eq!(small.len(), 63);
    }

    #[test]
    fn bitxor_large_with_serialized_small_deduplicates_rhs() {
        let mut lhs = HybridBitmap::from_iter(0_u64..64);
        let mut rhs = Vec::new();
        rhs.extend_from_slice(&HYBRID_MAGIC);
        rhs.push(HYBRID_VERSION);
        rhs.push(HYBRID_KIND_SMALL);
        rhs.push(3);
        for value in [5_u64, 5, 100] {
            rhs.extend_from_slice(&value.to_le_bytes());
        }

        lhs.bitxor_assign_rhs(BitmapRhs::Serialized(&rhs)).unwrap();

        assert!(!lhs.contains(5));
        assert!(lhs.contains(100));
        assert_eq!(lhs.len(), 64);
    }

    #[test]
    fn deserialize_small_lhs_deduplicates_before_large_rhs_xor() {
        let mut lhs_buf = Vec::new();
        lhs_buf.extend_from_slice(&HYBRID_MAGIC);
        lhs_buf.push(HYBRID_VERSION);
        lhs_buf.push(HYBRID_KIND_SMALL);
        lhs_buf.push(2);
        for value in [5_u64, 5] {
            lhs_buf.extend_from_slice(&value.to_le_bytes());
        }

        let mut lhs = deserialize_bitmap(&lhs_buf).unwrap();
        assert_eq!(lhs.len(), 1);

        lhs.bitxor_assign(HybridBitmap::from_iter(0_u64..64));

        assert!(!lhs.contains(5));
        assert!(lhs.contains(0));
        assert_eq!(lhs.len(), 63);
    }

    #[test]
    fn sub_small_with_large_stays_small() {
        let mut small = HybridBitmap::from_iter([1_u64, 5, 100]);
        let large = HybridBitmap::from_iter(0_u64..64);
        small.sub_assign(large);

        match small {
            HybridBitmap::Small(set) => assert_eq!(set.as_slice(), &[100]),
            _ => panic!("expected small hybrid bitmap after difference"),
        }
    }

    #[test]
    fn bitand_assign_serialized_small_rhs_uses_small_result() {
        let mut lhs = HybridBitmap::from_iter(0_u64..64);
        let rhs = HybridBitmap::from_iter([1_u64, 5, 100]);
        let mut rhs_buf = Vec::new();
        rhs.serialize_into(&mut rhs_buf).unwrap();

        lhs.bitand_assign_rhs(BitmapRhs::Serialized(&rhs_buf))
            .unwrap();

        match lhs {
            HybridBitmap::Small(set) => assert_eq!(set.as_slice(), &[1, 5]),
            _ => panic!("expected small hybrid bitmap after serialized intersection"),
        }
    }

    #[test]
    fn empty_lhs_validates_serialized_rhs() {
        let mut lhs = HybridBitmap::new();
        let bad_rhs = [HYBRID_MAGIC[0], HYBRID_MAGIC[1], HYBRID_VERSION, 42];

        assert!(
            lhs.bitand_assign_rhs(BitmapRhs::Serialized(&bad_rhs))
                .is_err()
        );
    }

    #[test]
    fn iterates_in_sorted_order() {
        let bitmap = HybridBitmap::from_iter([5_u64, 1, 3]);
        let values: Vec<_> = bitmap.iter().collect();
        assert_eq!(values, vec![1, 3, 5]);
    }

    #[test]
    fn small_bitmap_serialization_stays_compatible() {
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&HYBRID_MAGIC);
        legacy.push(HYBRID_VERSION);
        legacy.push(HYBRID_KIND_SMALL);
        legacy.push(3); // length in number of u64 values
        for value in [4_u64, 7, 42] {
            legacy.extend_from_slice(&value.to_le_bytes());
        }

        let decoded = deserialize_bitmap(&legacy).unwrap();
        match &decoded {
            HybridBitmap::Small(set) => assert_eq!(set.as_slice(), &[4, 7, 42]),
            _ => panic!("expected small hybrid bitmap"),
        }

        let mut reencoded = Vec::new();
        decoded.serialize_into(&mut reencoded).unwrap();
        assert_eq!(reencoded, legacy);
    }

    #[test]
    fn roaring_bytes_still_deserialize() {
        let mut tree = RoaringTreemap::new();
        tree.insert(1);
        tree.insert(5);
        tree.insert(42);

        let mut legacy = Vec::new();
        tree.serialize_into(&mut legacy).unwrap();

        let decoded = deserialize_bitmap(&legacy).unwrap();
        assert_eq!(decoded.into_iter().collect::<Vec<_>>(), vec![1, 5, 42]);
    }
}

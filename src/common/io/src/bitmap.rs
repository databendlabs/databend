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

    if is_hybrid_large(buf) {
        Ok(reader::bitmap_len(&buf[HYBRID_HEADER_LEN..])? as u64)
    } else {
        Ok(deserialize_bitmap(buf)?.len())
    }
}

fn is_hybrid(buf: &[u8]) -> bool {
    buf.len() >= HYBRID_HEADER_LEN && buf[..2] == HYBRID_MAGIC && buf[2] == HYBRID_VERSION
}

fn is_hybrid_large(buf: &[u8]) -> bool {
    is_hybrid(buf) && buf[3] == HYBRID_KIND_LARGE
}

fn as_roaring(buf: &[u8]) -> Option<&[u8]> {
    if is_hybrid_large(buf) {
        Some(&buf[HYBRID_HEADER_LEN..])
    } else if !is_hybrid(buf) {
        Some(buf) // Legacy
    } else {
        None // HybridSmall
    }
}

struct SmallReader {
    values: SmallBitmap,
}

impl SmallReader {
    // decode_small_payload returns raw bytes which may contain duplicates or be
    // unsorted; decode_small_values deduplicates and sorts via small_insert, so
    // that binary-search and two-pointer merge agree with deserialize_bitmap.
    fn new(buf: &[u8]) -> Result<Self> {
        let payload = &buf[HYBRID_HEADER_LEN..];
        Ok(Self {
            values: decode_small_values(payload)?,
        })
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn first(&self) -> Option<u64> {
        self.values.first().copied()
    }

    fn last(&self) -> Option<u64> {
        self.values.last().copied()
    }

    fn contains(&self, value: u64) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    fn has_any_with(&self, other: &SmallReader) -> bool {
        let mut i = 0;
        let mut j = 0;
        while i < self.len() && j < other.len() {
            let lv = self.values[i];
            let rv = other.values[j];
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

    fn has_all_with(&self, other: &SmallReader) -> bool {
        if self.len() < other.len() {
            return false;
        }
        let mut i = 0;
        let mut j = 0;
        while j < other.len() {
            if i >= self.len() {
                return false;
            }
            let lv = self.values[i];
            let rv = other.values[j];
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
}

/// Validate that a serialized bitmap buffer has minimum length.
///
/// Different from [`validate_serialized_bitmap`], this one does not check payload to be
/// lightweight.
pub(crate) fn validate_serialized_bitmap_header_and_length(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    } else if is_hybrid(buf) {
        match buf[3] {
            HYBRID_KIND_SMALL => {
                // HybridSmall payload: at least 1 byte (length byte)
                if buf.len() < HYBRID_HEADER_LEN + 1 {
                    return Err(ErrorCode::BadBytes("hybrid small bitmap: buffer too short"));
                }
            }
            HYBRID_KIND_LARGE => {
                // HybridLarge payload: at least 8 bytes (u64 prefix bucket count)
                if buf.len() < HYBRID_HEADER_LEN + 8 {
                    return Err(ErrorCode::BadBytes("hybrid large bitmap: buffer too short"));
                }
            }
            kind => {
                return Err(ErrorCode::BadBytes(format!(
                    "hybrid bitmap: invalid kind {kind}"
                )));
            }
        }
    } else if buf.len() < 8 {
        // Legacy RoaringTreemap: minimum 8 bytes (u64 prefix bucket count)
        return Err(ErrorCode::BadBytes("bitmap: buffer too short"));
    }

    Ok(())
}

pub fn bitmap_contains(buf: &[u8], value: u64) -> Result<bool> {
    // Fast path: empty bitmap contains nothing
    if buf.is_empty() {
        return Ok(false);
    }
    validate_serialized_bitmap_header_and_length(buf)?;
    if let Some(roaring_buf) = as_roaring(buf) {
        Ok(reader::TreemapReader::new(roaring_buf)?.contains(value)?)
    } else {
        Ok(SmallReader::new(buf)?.contains(value))
    }
}

pub fn bitmap_min(buf: &[u8]) -> Result<Option<u64>> {
    // Fast path: empty bitmap has no minimum
    if buf.is_empty() {
        return Ok(None);
    }
    validate_serialized_bitmap_header_and_length(buf)?;
    if let Some(roaring_buf) = as_roaring(buf) {
        Ok(reader::bitmap_min(roaring_buf)?)
    } else {
        Ok(SmallReader::new(buf)?.first())
    }
}

pub fn bitmap_max(buf: &[u8]) -> Result<Option<u64>> {
    // Fast path: empty bitmap has no maximum
    if buf.is_empty() {
        return Ok(None);
    }
    validate_serialized_bitmap_header_and_length(buf)?;
    if let Some(roaring_buf) = as_roaring(buf) {
        Ok(reader::bitmap_max(roaring_buf)?)
    } else {
        Ok(SmallReader::new(buf)?.last())
    }
}

pub fn bitmap_has_all(lhs: &[u8], rhs: &[u8]) -> Result<bool> {
    // Fast path: empty rhs is subset of anything
    if rhs.is_empty() {
        return Ok(true);
    }
    // Fast path: empty lhs cannot contain non-empty rhs
    if lhs.is_empty() {
        return Ok(bitmap_len(rhs)? == 0);
    }
    validate_serialized_bitmap_header_and_length(lhs)?;
    validate_serialized_bitmap_header_and_length(rhs)?;

    // Both HybridLarge or Legacy: use visitor (zero-copy)
    if let (Some(lhs_buf), Some(rhs_buf)) = (as_roaring(lhs), as_roaring(rhs)) {
        return Ok(reader::bitmap_has_all(lhs_buf, rhs_buf)?);
    }

    // Both HybridSmall: two-pointer subset check
    if as_roaring(lhs).is_none() && as_roaring(rhs).is_none() {
        let lhs = SmallReader::new(lhs)?;
        let rhs = SmallReader::new(rhs)?;
        return Ok(lhs.has_all_with(&rhs));
    }

    // Fast path: HybridSmall lhs, HybridLarge or Legacy rhs cardinality check
    // We need to check cardinality here because: (1) it might be a legacy tree,
    // and (2) we don't always demote HybridBitmap after reducing its cardinality immediately.
    if as_roaring(lhs).is_none() && as_roaring(rhs).is_some() {
        let lhs_small = SmallReader::new(lhs)?;
        let rhs_roaring = as_roaring(rhs).unwrap();
        // Fast path: rhs has more values than lhs, impossible to contain
        if reader::bitmap_len_above(rhs_roaring, lhs_small.len())? {
            return Ok(false);
        }
        // rhs has <= lhs_small.len() values, but still need to check containment
        let lhs_bm = deserialize_bitmap(lhs)?;
        let rhs_bm = deserialize_bitmap(rhs)?;
        return Ok(lhs_bm.is_superset(&rhs_bm));
    }

    // lhs is HybridLarge/Legacy, rhs is HybridSmall: probe
    let roaring_buf = as_roaring(lhs).unwrap();
    let rhs_small = SmallReader::new(rhs)?;
    let tree = reader::TreemapReader::new(roaring_buf)?;
    for i in 0..rhs_small.len() {
        if !tree.contains(rhs_small.values[i])? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn bitmap_has_any(lhs: &[u8], rhs: &[u8]) -> Result<bool> {
    // Fast path: empty bitmap has no intersection
    if lhs.is_empty() || rhs.is_empty() {
        return Ok(false);
    }
    validate_serialized_bitmap_header_and_length(lhs)?;
    validate_serialized_bitmap_header_and_length(rhs)?;

    // Both HybridLarge or Legacy: use visitor (zero-copy)
    if let (Some(lhs_buf), Some(rhs_buf)) = (as_roaring(lhs), as_roaring(rhs)) {
        return Ok(reader::bitmap_has_any(lhs_buf, rhs_buf)?);
    }

    // Both HybridSmall: two-pointer intersection
    if as_roaring(lhs).is_none() && as_roaring(rhs).is_none() {
        let lhs = SmallReader::new(lhs)?;
        let rhs = SmallReader::new(rhs)?;
        return Ok(lhs.has_any_with(&rhs));
    }

    // One side HybridLarge/Legacy, the other HybridSmall: probe
    let (roaring_buf, small) = if let Some(rb) = as_roaring(lhs) {
        (rb, SmallReader::new(rhs)?)
    } else {
        (as_roaring(rhs).unwrap(), SmallReader::new(lhs)?)
    };
    let tree = reader::TreemapReader::new(roaring_buf)?;
    for i in 0..small.len() {
        if tree.contains(small.values[i])? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_bitmap_rhs(buf: &[u8]) -> Result<BitmapRhsView<'_>> {
    if buf.is_empty() {
        return Ok(BitmapRhsView::Empty);
    }

    if is_hybrid(buf) {
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

    if is_hybrid(buf) {
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

    // A corrupt large buffer paired with an empty HybridSmall must surface
    // the decoding error, not silently return `false`/`true`. Without eager
    // validation of the roaring side, the mixed-path loop runs zero times
    // (small side is empty) and the corrupt large side slips through.
    #[test]
    fn has_any_corrupt_large_with_empty_small_is_rejected() {
        let corrupt_large = 1u64.to_le_bytes(); // declares 1 bucket, no data
        let empty_small = [
            HYBRID_MAGIC[0],
            HYBRID_MAGIC[1],
            HYBRID_VERSION,
            HYBRID_KIND_SMALL,
            0, // 0 values
        ];

        assert!(bitmap_has_any(&corrupt_large, &empty_small).is_err());
        assert!(bitmap_has_any(&empty_small, &corrupt_large).is_err());
    }

    #[test]
    fn has_all_corrupt_large_with_empty_small_is_rejected() {
        let corrupt_large = 1u64.to_le_bytes();
        let empty_small = [
            HYBRID_MAGIC[0],
            HYBRID_MAGIC[1],
            HYBRID_VERSION,
            HYBRID_KIND_SMALL,
            0,
        ];

        assert!(bitmap_has_all(&corrupt_large, &empty_small).is_err());
        assert!(bitmap_has_all(&empty_small, &corrupt_large).is_err());
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
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

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

    fn make_buf(tree: &RoaringTreemap) -> Vec<u8> {
        let bm = HybridBitmap::from_iter(tree.iter());
        let mut buf = Vec::new();
        bm.serialize_into(&mut buf).unwrap();
        buf
    }

    fn for_each_fixture<F>(mut f: F)
    where F: FnMut(&str, &[u8], &RoaringTreemap, u64) {
        let fixtures: Vec<(&str, RoaringTreemap, u64)> = vec![
            // HL multi-prefix random
            ("random", create_bitmap(123), u64::MAX),
            // HS single-prefix
            ("format: HS", (0..31u64).collect(), 50),
            // HS multi-prefix sparse
            (
                "format: HS multi-prefix",
                RoaringTreemap::from_iter([0u64, 65535, 65536, (1u64 << 32) + 500]),
                1,
            ),
            // boundary: 4095 (array threshold-)
            ("boundary: 4095", (0..4095u64).collect(), 4095),
            // boundary: 4096 (array threshold)
            ("boundary: 4096", (0..4096u64).collect(), 4096),
            // boundary: 4097 (array threshold+, becomes bitmap)
            ("boundary: 4097", (0..4097u64).collect(), 4097),
            // boundary: 65535 (near-full container)
            ("boundary: 65535", (0..65535u64).collect(), 65535),
            // boundary: 65536 (full container)
            ("boundary: 65536", (0..65536u64).collect(), 65536),
            // boundary: 65537 (full+ container, splits to 2 containers)
            ("boundary: 65537", (0..65537u64).collect(), 65537),
        ];
        for (name, tree, miss) in fixtures {
            let buf = make_buf(&tree);
            f(name, &buf, &tree, miss);
        }
        // empty buffer
        f("format: empty", &[], &RoaringTreemap::new(), 0);
        // HE serialized empty
        let he_tree = RoaringTreemap::new();
        let he_buf = make_buf(&he_tree);
        f("format: HE", &he_buf, &he_tree, 0);
    }

    fn for_each_fixture_pair<F>(mut f: F)
    where F: FnMut(&str, &[u8], &[u8], &RoaringTreemap, &RoaringTreemap) {
        let scenarios: Vec<(&str, RoaringTreemap, RoaringTreemap)> = vec![
            // lhs: HL | rhs: HL overlap
            (
                "format: HL+HL overlap",
                (0..50000u64).collect(),
                (30000..80000u64).collect(),
            ),
            // lhs: HL | rhs: HL disjoint
            (
                "format: HL+HL disjoint",
                (0..50000u64).collect(),
                (100000..150000u64).collect(),
            ),
            // lhs: HL | rhs: HL superset
            (
                "format: HL+HL superset",
                (0..50000u64).collect(),
                (0..100u64).collect(),
            ),
            // lhs: HL | rhs: HL not superset
            (
                "format: HL+HL not superset",
                (0..50000u64).collect(),
                (40000..90000u64).collect(),
            ),
            // lhs: HL | rhs: HL self
            (
                "format: HL+HL self",
                (0..50000u64).collect(),
                (0..50000u64).collect(),
            ),
            // lhs: HL | rhs: HS subset
            (
                "format: HL+HS subset",
                (0..50000u64).collect(),
                (5..15u64).collect(),
            ),
            // lhs: HL | rhs: HS not subset
            (
                "format: HL+HS not subset",
                (0..50000u64).collect(),
                (49990..50010u64).collect(),
            ),
            // lhs: HS | rhs: HL overlap
            (
                "format: HS+HL overlap",
                (0..31u64).collect(),
                (0..50000u64).collect(),
            ),
            // lhs: HS | rhs: HL disjoint
            (
                "format: HS+HL disjoint",
                (0..31u64).collect(),
                (100000..150000u64).collect(),
            ),
            // lhs: HS | rhs: HS overlap
            (
                "format: HS+HS overlap",
                (0..31u64).collect(),
                (20..51u64).collect(),
            ),
            // lhs: HS | rhs: HS disjoint
            (
                "format: HS+HS disjoint",
                (0..15u64).collect(),
                (20..35u64).collect(),
            ),
            // lhs: HS | rhs: HS subset
            (
                "format: HS+HS subset",
                (0..31u64).collect(),
                (5..15u64).collect(),
            ),
            // lhs: HS | rhs: HS not subset
            (
                "format: HS+HS not subset",
                (0..15u64).collect(),
                (10..25u64).collect(),
            ),
            // lhs: p0 | rhs: p0, p1 rhs-only prefix
            (
                "prefix: rhs-only",
                (0u64..0x10000).step_by(7).collect(),
                (0u64..0x10000)
                    .step_by(5)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(5))
                    .collect(),
            ),
            // lhs: p0, p1 | rhs: p0 lhs-only prefix
            (
                "prefix: lhs-only",
                (0u64..0x10000)
                    .step_by(7)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(7))
                    .collect(),
                (0u64..0x10000).step_by(5).collect(),
            ),
            // lhs: p0 | rhs: p1 disjoint prefixes
            (
                "prefix: disjoint",
                (0u64..0x10000).step_by(7).collect(),
                ((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(5).collect(),
            ),
            // lhs: p0, p1 | rhs: p1, p2 overlap prefixes
            (
                "prefix: overlap",
                (0u64..0x10000)
                    .step_by(7)
                    .chain(((1u64 << 32)..(1u64 << 32) + 0x10000).step_by(7))
                    .collect(),
                ((1u64 << 32)..(1u64 << 32) + 0x10000)
                    .step_by(5)
                    .chain(((2u64 << 32)..(2u64 << 32) + 0x10000).step_by(5))
                    .collect(),
            ),
            // boundary: 4095 + 4096
            (
                "boundary: 4095+4096",
                (0..4095u64).collect(),
                (0..4096u64).collect(),
            ),
            // boundary: 4096 + 4097
            (
                "boundary: 4096+4097",
                (0..4096u64).collect(),
                (0..4097u64).collect(),
            ),
            // boundary: 4097 + 4097
            (
                "boundary: 4097+4097",
                (0..4097u64).collect(),
                (0..4097u64).collect(),
            ),
            // boundary: 65535 + 65536
            (
                "boundary: 65535+65536",
                (0..65535u64).collect(),
                (0..65536u64).collect(),
            ),
            // boundary: 65536 + 65536
            (
                "boundary: 65536+65536",
                (0..65536u64).collect(),
                (0..65536u64).collect(),
            ),
            // boundary: 65537 + 65537
            (
                "boundary: 65537+65537",
                (0..65537u64).collect(),
                (0..65537u64).collect(),
            ),
            // boundary: 65535 + 65537
            (
                "boundary: 65535+65537",
                (0..65535u64).collect(),
                (0..65537u64).collect(),
            ),
            // boundary: 4095 + 65535
            (
                "boundary: 4095+65535",
                (0..4095u64).collect(),
                (0..65535u64).collect(),
            ),
            // boundary: 4095 + 65536
            (
                "boundary: 4095+65536",
                (0..4095u64).collect(),
                (0..65536u64).collect(),
            ),
            // boundary: 4095 + 65537
            (
                "boundary: 4095+65537",
                (0..4095u64).collect(),
                (0..65537u64).collect(),
            ),
            // boundary: 4096 + 65535
            (
                "boundary: 4096+65535",
                (0..4096u64).collect(),
                (0..65535u64).collect(),
            ),
            // boundary: 4096 + 65536
            (
                "boundary: 4096+65536",
                (0..4096u64).collect(),
                (0..65536u64).collect(),
            ),
            // boundary: 4096 + 65537
            (
                "boundary: 4096+65537",
                (0..4096u64).collect(),
                (0..65537u64).collect(),
            ),
            // boundary: 4097 + 65535
            (
                "boundary: 4097+65535",
                (0..4097u64).collect(),
                (0..65535u64).collect(),
            ),
            // boundary: 4097 + 65536
            (
                "boundary: 4097+65536",
                (0..4097u64).collect(),
                (0..65536u64).collect(),
            ),
            // boundary: 4097 + 65537
            (
                "boundary: 4097+65537",
                (0..4097u64).collect(),
                (0..65537u64).collect(),
            ),
        ];
        for (name, lhs_tree, rhs_tree) in scenarios {
            let lhs_buf = make_buf(&lhs_tree);
            let rhs_buf = make_buf(&rhs_tree);
            f(name, &lhs_buf, &rhs_buf, &lhs_tree, &rhs_tree);
        }
        // Empty scenarios
        let hl_tree: RoaringTreemap = (0..50000u64).collect();
        let hl_buf = make_buf(&hl_tree);
        let hs_tree: RoaringTreemap = (0..31u64).collect();
        let hs_buf = make_buf(&hs_tree);
        let he_tree: RoaringTreemap = RoaringTreemap::new();
        let he_buf = make_buf(&he_tree);
        // lhs: empty | rhs: HL
        f(
            "format: Empty+HL",
            &[],
            &hl_buf,
            &RoaringTreemap::new(),
            &hl_tree,
        );
        // lhs: HE | rhs: HL
        f("format: HE+HL", &he_buf, &hl_buf, &he_tree, &hl_tree);
        // lhs: empty | rhs: HS
        f(
            "format: Empty+HS",
            &[],
            &hs_buf,
            &RoaringTreemap::new(),
            &hs_tree,
        );
        // lhs: HE | rhs: HS
        f("format: HE+HS", &he_buf, &hs_buf, &he_tree, &hs_tree);
        // lhs: HL | rhs: empty
        f(
            "format: HL+Empty",
            &hl_buf,
            &[],
            &hl_tree,
            &RoaringTreemap::new(),
        );
        // lhs: HL | rhs: HE
        f("format: HL+HE", &hl_buf, &he_buf, &hl_tree, &he_tree);
        // lhs: HS | rhs: empty
        f(
            "format: HS+Empty",
            &hs_buf,
            &[],
            &hs_tree,
            &RoaringTreemap::new(),
        );
        // lhs: HS | rhs: HE
        f("format: HS+HE", &hs_buf, &he_buf, &hs_tree, &he_tree);
        // lhs: empty | rhs: empty
        f(
            "format: Empty+Empty",
            &[],
            &[],
            &RoaringTreemap::new(),
            &RoaringTreemap::new(),
        );
        // lhs: empty | rhs: HE
        f(
            "format: Empty+HE",
            &[],
            &he_buf,
            &RoaringTreemap::new(),
            &he_tree,
        );
        // lhs: HE | rhs: empty
        f(
            "format: HE+Empty",
            &he_buf,
            &[],
            &he_tree,
            &RoaringTreemap::new(),
        );
        // lhs: HE | rhs: HE
        f("format: HE+HE", &he_buf, &he_buf, &he_tree, &he_tree);
    }

    #[test]
    fn test_bitmap_contains() {
        for_each_fixture(|name, buf, tree, miss_value| {
            if let Some(hit) = tree.min() {
                let expected = tree.contains(hit);
                let actual = bitmap_contains(buf, hit).unwrap();
                assert_eq!(
                    actual, expected,
                    "bitmap_contains hit: fixture={name}, val={hit}"
                );
            }
            let miss = miss_value;
            let expected = tree.contains(miss);
            let actual = bitmap_contains(buf, miss).unwrap();
            assert_eq!(
                actual, expected,
                "bitmap_contains miss: fixture={name}, val={miss}"
            );
        });
    }

    #[test]
    fn test_bitmap_min() {
        for_each_fixture(|name, buf, tree, _miss_value| {
            let expected = tree.min();
            let actual = bitmap_min(buf).unwrap();
            assert_eq!(actual, expected, "bitmap_min: fixture={name}");
        });
    }

    #[test]
    fn test_bitmap_max() {
        for_each_fixture(|name, buf, tree, _miss_value| {
            let expected = tree.max();
            let actual = bitmap_max(buf).unwrap();
            assert_eq!(actual, expected, "bitmap_max: fixture={name}");
        });
    }

    #[test]
    fn test_bitmap_has_any() {
        for_each_fixture_pair(|name, lhs_buf, rhs_buf, lhs_tree, rhs_tree| {
            let expected = !(lhs_tree & rhs_tree).is_empty();
            let actual = bitmap_has_any(lhs_buf, rhs_buf).unwrap();
            assert_eq!(actual, expected, "bitmap_has_any: fixture={name}");
        });
    }

    #[test]
    fn test_bitmap_has_all() {
        for_each_fixture_pair(|name, lhs_buf, rhs_buf, lhs_tree, rhs_tree| {
            let expected = lhs_tree.is_superset(rhs_tree);
            let actual = bitmap_has_all(lhs_buf, rhs_buf).unwrap();
            assert_eq!(actual, expected, "bitmap_has_all: fixture={name}");
        });
    }

    /// proptests for bitmap functions:
    ///
    ///  - [x] bitmap_contains
    ///  - [x] bitmap_min
    ///  - [x] bitmap_max
    ///  - [x] bitmap_has_any
    ///  - [x] bitmap_has_all
    ///
    /// According to comment in src/common/column/tests/it/bitmap/assign_ops.rs,
    /// following prop tests are ignored when using `miri`.
    #[cfg_attr(miri, ignore)]
    mod proptests {
        use proptest::bits::BitSetLike;
        use proptest::bits::SampledBitSetStrategy;
        use proptest::collection::SizeRange;
        use proptest::collection::btree_map;
        use proptest::prelude::*;
        use roaring::RoaringBitmap;

        use super::*;

        /// The random bits strategy.
        ///
        /// Generate random bits with [`Store::sampled`].
        #[derive(Clone, Debug)]
        struct Store(Vec<u16>);

        impl Store {
            fn sampled(
                size: impl Into<SizeRange>,
                bits: impl Into<SizeRange>,
            ) -> SampledBitSetStrategy<Self> {
                SampledBitSetStrategy::new(size.into(), bits.into())
            }
        }

        /// Implement BitSetLike as required by SampledBitSetStrategy.
        impl BitSetLike for Store {
            fn new_bitset(max: usize) -> Self {
                assert!(max <= u16::MAX as usize + 1);
                Store(Vec::new())
            }
            fn len(&self) -> usize {
                u16::MAX as usize + 1
            }
            fn test(&self, bit: usize) -> bool {
                self.0.binary_search(&(bit as u16)).is_ok()
            }
            fn set(&mut self, bit: usize) {
                let v = bit as u16;
                if let Err(pos) = self.0.binary_search(&v) {
                    self.0.insert(pos, v);
                }
            }
            fn clear(&mut self, bit: usize) {
                let v = bit as u16;
                if let Ok(pos) = self.0.binary_search(&v) {
                    self.0.remove(pos);
                }
            }
            fn count(&self) -> usize {
                self.0.len()
            }
        }

        /// The container strategy.
        ///
        /// Generates:
        ///     50% array container with 1 to 4096 values
        ///     50% bitmap container with 4097 to 65536 values
        fn container_strategy() -> impl Strategy<Value = Vec<u16>> {
            prop_oneof![
                Store::sampled(1..=4096, ..=u16::MAX as usize).prop_map(|bs| bs.0),
                Store::sampled(4097..u16::MAX as usize, ..=u16::MAX as usize).prop_map(|bs| bs.0),
            ]
        }

        /// The RoaringBitmap strategy.
        ///
        /// Generate 0 to 16 containers, with containers from [`container_strategy`].
        fn bitmap_strategy() -> impl Strategy<Value = RoaringBitmap> {
            btree_map(0u16..=16, container_strategy(), 0usize..=16).prop_map(|map| {
                let mut bitmap = RoaringBitmap::new();
                for (key, values) in map {
                    for v in values {
                        bitmap.insert((key as u32) << 16 | v as u32);
                    }
                }
                bitmap
            })
        }

        /// The RoaringTreemap strategy.
        ///
        /// Generate 0 to 16 RoaringBitmaps, each generated by [`bitmap_strategy`].
        fn tree_strategy() -> impl Strategy<Value = RoaringTreemap> {
            btree_map(0u32..=16, bitmap_strategy(), 0usize..=16).prop_map(|map| {
                let mut treemap = RoaringTreemap::new();
                for (key, bitmap) in map {
                    if !bitmap.is_empty() {
                        for v in bitmap.iter() {
                            treemap.insert((key as u64) << 32 | v as u64);
                        }
                    }
                }
                treemap
            })
        }

        /// The serialization strategy.
        ///
        /// Serialize RoaringTreemap from [`tree_strategy`] into `Vec<u8>`:
        ///     80% HybridBitmap
        ///     10% Legacy
        ///     10% Empty
        fn serialization_strategy() -> impl Strategy<Value = (Vec<u8>, RoaringTreemap)> {
            prop_oneof![
                8 => tree_strategy().prop_map(|tree| {
                    let bm = HybridBitmap::from_iter(tree.iter());
                    let mut buf = Vec::new();
                    bm.serialize_into(&mut buf).unwrap();
                    (buf, tree)
                }),
                1 => tree_strategy().prop_map(|tree| {
                    let mut buf = Vec::new();
                    tree.serialize_into(&mut buf).unwrap();
                    (buf, tree)
                }),
                1 => Just((Vec::new(), RoaringTreemap::new())),
            ]
        }

        /// The probe strategy, picks random value for bitmap_contains to probe.
        ///
        /// 50% hit: probe = random value from tree (via nth)
        /// 50% miss: probe = random u64
        fn probe_strategy() -> impl Strategy<Value = (Vec<u8>, RoaringTreemap, u64)> {
            (serialization_strategy(), any::<bool>(), any::<u64>()).prop_map(
                |((buf, tree), is_hit, random_value)| {
                    let probe = if is_hit && !tree.is_empty() {
                        let idx = (random_value % tree.len()) as usize;
                        tree.iter().nth(idx).unwrap_or(random_value)
                    } else {
                        random_value
                    };
                    (buf, tree, probe)
                },
            )
        }

        proptest! {
            // Make the test run faster by limiting the number of cases, running 32 cases
            // took ~16 seconds in release, and ~160 in debug.
            // One can override this by setting the `PROPTEST_CASES` environment variable.
            #![proptest_config(ProptestConfig::with_cases(32))]

            #[test]
            fn prop_bitmap_contains((buf, tree, value) in probe_strategy()) {
                let expected = tree.contains(value);
                let actual = bitmap_contains(&buf, value).unwrap();
                assert_eq!(actual, expected);
            }

            #[test]
            fn prop_bitmap_min((buf, tree) in serialization_strategy()) {
                assert_eq!(bitmap_min(&buf).unwrap(), tree.min());
            }

            #[test]
            fn prop_bitmap_max((buf, tree) in serialization_strategy()) {
                assert_eq!(bitmap_max(&buf).unwrap(), tree.max());
            }

            #[test]
            fn prop_bitmap_has_any(
                ((lhs_buf, lhs_tree), (rhs_buf, rhs_tree)) in (serialization_strategy(), serialization_strategy())
            ) {
                let expected = !(lhs_tree & rhs_tree).is_empty();
                let actual = bitmap_has_any(&lhs_buf, &rhs_buf).unwrap();
                assert_eq!(actual, expected);
            }

            #[test]
            fn prop_bitmap_has_all(
                ((lhs_buf, lhs_tree), (rhs_buf, rhs_tree)) in (serialization_strategy(), serialization_strategy())
            ) {
                let expected = lhs_tree.is_superset(&rhs_tree);
                let actual = bitmap_has_all(&lhs_buf, &rhs_buf).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
}

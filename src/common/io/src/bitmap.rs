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

use std::collections::btree_set;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::iter::FromIterator;
use std::mem;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use roaring::RoaringTreemap;

// https://github.com/ClickHouse/ClickHouse/blob/516a6ed6f8bd8c5f6eed3a10e9037580b2fb6152/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L914
const LARGE_THRESHOLD: usize = 32;
const HYBRID_MAGIC: [u8; 2] = *b"HB";
const HYBRID_VERSION: u8 = 1;
const HYBRID_KIND_SMALL: u8 = 0;
const HYBRID_KIND_LARGE: u8 = 1;
const HYBRID_HEADER_LEN: usize = 4;

#[derive(Clone)]
pub enum HybridBitmap {
    Small(BTreeSet<u64>),
    Large(RoaringTreemap),
}

impl Default for HybridBitmap {
    fn default() -> Self {
        HybridBitmap::Small(BTreeSet::new())
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
        match self {
            HybridBitmap::Small(set) => set.insert(value),
            HybridBitmap::Large(tree) => tree.insert(value),
        }
    }

    pub fn contains(&self, value: u64) -> bool {
        match self {
            HybridBitmap::Small(set) => set.contains(&value),
            HybridBitmap::Large(tree) => tree.contains(value),
        }
    }

    pub fn max(&self) -> Option<u64> {
        match self {
            HybridBitmap::Small(set) => set.iter().copied().max(),
            HybridBitmap::Large(tree) => tree.max(),
        }
    }

    pub fn min(&self) -> Option<u64> {
        match self {
            HybridBitmap::Small(set) => set.iter().copied().min(),
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
            (HybridBitmap::Small(lhs), HybridBitmap::Small(rhs)) => lhs.is_superset(rhs),
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
                lhs.intersection(rhs).count() as u64
            }
        }
    }

    pub fn serialize_into<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&HYBRID_MAGIC)?;
        writer.write_all(&[HYBRID_VERSION])?;
        match self {
            HybridBitmap::Small(set) if set.len() > LARGE_THRESHOLD => {
                writer.write_all(&[HYBRID_KIND_LARGE])?;
                let tree = RoaringTreemap::from_iter(set.iter().copied());
                tree.serialize_into(writer)
            }
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
                inner: HybridBitmapIterInner::Large(Box::new(tree.into_iter())),
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
        if let HybridBitmap::Large(tree) = self {
            if (tree.len() as usize) <= LARGE_THRESHOLD {
                let data = mem::take(tree);
                let mut set = BTreeSet::new();
                for value in data.into_iter() {
                    set.insert(value);
                }
                *self = HybridBitmap::Small(set);
            }
        }
    }
}

impl From<RoaringTreemap> for HybridBitmap {
    fn from(value: RoaringTreemap) -> Self {
        if (value.len() as usize) <= LARGE_THRESHOLD {
            let mut set = BTreeSet::new();
            for v in value.into_iter() {
                set.insert(v);
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
        match rhs {
            HybridBitmap::Large(rhs_tree) => {
                let lhs_tree = self.promote_to_tree();
                lhs_tree.bitor_assign(rhs_tree);
            }
            HybridBitmap::Small(rhs_set) => match self {
                HybridBitmap::Large(lhs_tree) => {
                    for value in rhs_set {
                        lhs_tree.insert(value);
                    }
                }
                HybridBitmap::Small(lhs_set) => {
                    lhs_set.extend(rhs_set);
                }
            },
        }
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
        match rhs {
            HybridBitmap::Large(rhs_tree) => {
                let lhs_tree = self.promote_to_tree();
                lhs_tree.bitand_assign(rhs_tree);
                self.try_demote();
            }
            HybridBitmap::Small(rhs_set) => match self {
                HybridBitmap::Large(lhs_tree) => {
                    let rhs_tree = RoaringTreemap::from_iter(rhs_set);
                    lhs_tree.bitand_assign(rhs_tree);
                    self.try_demote();
                }
                HybridBitmap::Small(lhs_set) => {
                    lhs_set.retain(|value| rhs_set.contains(value));
                }
            },
        }
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
        match rhs {
            HybridBitmap::Large(rhs_tree) => {
                let lhs_tree = self.promote_to_tree();
                lhs_tree.bitxor_assign(rhs_tree);
                self.try_demote();
            }
            HybridBitmap::Small(rhs_set) => match self {
                HybridBitmap::Large(lhs_tree) => {
                    let rhs_tree = RoaringTreemap::from_iter(rhs_set);
                    lhs_tree.bitxor_assign(rhs_tree);
                    self.try_demote();
                }
                HybridBitmap::Small(lhs_set) => {
                    for value in rhs_set {
                        if !lhs_set.insert(value) {
                            lhs_set.remove(&value);
                        }
                    }
                }
            },
        }
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
        match rhs {
            HybridBitmap::Large(rhs_tree) => {
                let lhs_tree = self.promote_to_tree();
                lhs_tree.sub_assign(rhs_tree);
                self.try_demote();
            }
            HybridBitmap::Small(rhs_set) => match self {
                HybridBitmap::Large(lhs_tree) => {
                    let rhs_tree = RoaringTreemap::from_iter(rhs_set);
                    lhs_tree.sub_assign(rhs_tree);
                    self.try_demote();
                }
                HybridBitmap::Small(lhs_set) => {
                    let new_set = lhs_set.difference(&rhs_set).copied().collect();
                    *lhs_set = new_set;
                }
            },
        }
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
    Large(Box<<&'a RoaringTreemap as IntoIterator>::IntoIter>),
    Small(btree_set::Iter<'a, u64>),
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
    Small(btree_set::IntoIter<u64>),
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
                inner: HybridBitmapIntoIterInner::Small(set.into_iter()),
            },
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

    RoaringTreemap::deserialize_from(buf)
        .map(HybridBitmap::from)
        .map_err(|e| {
            let len = buf.len();
            let msg = format!("fail to decode bitmap from buffer of size {len}: {e}");
            ErrorCode::BadBytes(msg)
        })
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
            RoaringTreemap::deserialize_from(payload)
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

    let mut set = BTreeSet::new();
    for chunk in bytes.chunks_exact(std::mem::size_of::<u64>()) {
        let mut data = [0u8; std::mem::size_of::<u64>()];
        data.copy_from_slice(chunk);
        set.insert(u64::from_le_bytes(data));
    }
    Ok(HybridBitmap::Small(set))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn iterates_in_sorted_order() {
        let bitmap = HybridBitmap::from_iter([5_u64, 1, 3]);
        let values: Vec<_> = bitmap.iter().collect();
        assert_eq!(values, vec![1, 3, 5]);
    }

    #[test]
    fn serialize_promotes_large_representation() {
        let mut bitmap = HybridBitmap::new();
        for i in 0..=(LARGE_THRESHOLD as u64) {
            bitmap.insert(i);
        }
        assert!(matches!(bitmap, HybridBitmap::Small(_)));

        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf).unwrap();
        let decoded = deserialize_bitmap(&buf).unwrap();
        assert!(matches!(decoded, HybridBitmap::Large(_)));
    }
}

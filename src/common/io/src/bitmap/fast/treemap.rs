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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::io;
use std::io::Cursor;
use std::mem;

use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use roaring::RoaringBitmap;
use roaring::RoaringTreemap;

use crate::bitmap::HYBRID_HEADER_LEN;
use crate::bitmap::HYBRID_KIND_LARGE;
use crate::bitmap::HYBRID_KIND_SMALL;
use crate::bitmap::HYBRID_MAGIC;
use crate::bitmap::HYBRID_VERSION;
use crate::bitmap::HybridBitmap;
use crate::bitmap::LARGE_THRESHOLD;
use crate::bitmap::decode_small_values;
use crate::bitmap::reader::TreemapReader;

#[derive(Clone, Copy, Debug)]
enum FastTreemapOp {
    And,
    Or,
    Sub,
    Xor,
}

#[derive(Default)]
pub struct FastTreemap {
    // Invariant: every RoaringBitmap in the map is not empty.
    // Any code that may empty a value (e.g. Xor/Sub) must remove the key instead of
    // leaving an empty RoaringBitmap behind.
    map: BTreeMap<u32, RoaringBitmap>,
}

impl FastTreemap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> u64 {
        self.map.values().map(RoaringBitmap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, value: u64) {
        let prefix = (value >> 32) as u32;
        self.map.entry(prefix).or_default().insert(value as u32);
    }

    pub fn serialize_into<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&HYBRID_MAGIC)?;
        writer.write_all(&[HYBRID_VERSION])?;
        let len = self.len();
        if len <= LARGE_THRESHOLD as u64 {
            writer.write_all(&[HYBRID_KIND_SMALL])?;
            writer.write_all(&[len as u8])?;
            for (&prefix, bitmap) in &self.map {
                for low in bitmap.iter() {
                    let value = (u64::from(prefix) << 32) | u64::from(low);
                    writer.write_all(&value.to_le_bytes())?;
                }
            }
            return Ok(());
        }
        writer.write_all(&[HYBRID_KIND_LARGE])?;
        writer.write_u64::<LittleEndian>(self.map.len() as u64)?;
        for (&prefix, bitmap) in &self.map {
            writer.write_u32::<LittleEndian>(prefix)?;
            bitmap.serialize_into(&mut writer)?;
        }
        Ok(())
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        let mut bitmap = Self::new();
        bitmap.bitor_assign_serialized(buf)?;
        Ok(bitmap)
    }

    /// Computes the And of `self` and a serialized bitmap.
    /// Keeps only the values that also appear in `buf`.
    ///
    /// # Errors
    ///
    /// Malformed input returns an error; the map contents are then
    /// unspecified (untouched, partially merged, or empty).
    pub fn bitand_assign_serialized(&mut self, buf: &[u8]) -> Result<()> {
        self.apply_serialized(FastTreemapOp::And, buf)
    }

    /// Computes the Or of `self` and a serialized bitmap.
    /// Inserts every value of `buf`.
    ///
    /// # Errors
    ///
    /// Malformed input returns an error; the map contents are then
    /// unspecified (untouched, partially merged, or empty).
    pub fn bitor_assign_serialized(&mut self, buf: &[u8]) -> Result<()> {
        self.apply_serialized(FastTreemapOp::Or, buf)
    }

    /// Computes the Sub of `self` and a serialized bitmap.
    /// Removes every value of `buf`.
    ///
    /// # Errors
    ///
    /// Malformed input returns an error; the map contents are then
    /// unspecified (untouched, partially merged, or empty).
    pub fn sub_assign_serialized(&mut self, buf: &[u8]) -> Result<()> {
        self.apply_serialized(FastTreemapOp::Sub, buf)
    }

    /// Computes the Xor of `self` and a serialized bitmap.
    /// Flips membership for every value of `buf`.
    ///
    /// # Errors
    ///
    /// Malformed input returns an error; the map contents are then
    /// unspecified (untouched, partially merged, or empty).
    pub fn bitxor_assign_serialized(&mut self, buf: &[u8]) -> Result<()> {
        self.apply_serialized(FastTreemapOp::Xor, buf)
    }

    /// Computes the And of `self` and `rhs`.
    /// Keeps only the values that also appear in `rhs`.
    pub fn bitand_assign(&mut self, rhs: &Self) {
        self.apply_fast(FastTreemapOp::And, rhs);
    }

    /// Computes the Or of `self` and `rhs`.
    /// Inserts every value of `rhs`.
    pub fn bitor_assign(&mut self, rhs: &Self) {
        self.apply_fast(FastTreemapOp::Or, rhs);
    }

    /// Computes the Sub of `self` and `rhs`.
    /// Removes every value of `rhs`.
    pub fn sub_assign(&mut self, rhs: &Self) {
        self.apply_fast(FastTreemapOp::Sub, rhs);
    }

    /// Computes the Xor of `self` and `rhs`.
    /// Flips membership for every value of `rhs`.
    pub fn bitxor_assign(&mut self, rhs: &Self) {
        self.apply_fast(FastTreemapOp::Xor, rhs);
    }

    /// Computes the And of `self` and a Hybrid `rhs`.
    pub fn bitand_assign_hybrid(&mut self, rhs: &HybridBitmap) {
        let HybridBitmap::Large(tree) = rhs else {
            self.bitand_assign(&Self::from_hybrid(rhs));
            return;
        };
        let mut rhs_bitmaps = tree.bitmaps().peekable();
        self.map.retain(|prefix, bitmap| {
            while rhs_bitmaps.peek().is_some_and(|&(p, _)| p < *prefix) {
                rhs_bitmaps.next();
            }
            match rhs_bitmaps.peek() {
                Some(&(p, rhs_bitmap)) if p == *prefix => {
                    *bitmap &= rhs_bitmap;
                    !bitmap.is_empty()
                }
                _ => false,
            }
        });
    }

    /// Computes the Or of `self` and a Hybrid `rhs`.
    pub fn bitor_assign_hybrid(&mut self, rhs: &HybridBitmap) {
        let HybridBitmap::Large(tree) = rhs else {
            self.bitor_assign(&Self::from_hybrid(rhs));
            return;
        };
        for (prefix, rhs_bitmap) in tree.bitmaps() {
            match self.map.entry(prefix) {
                Entry::Vacant(entry) => {
                    entry.insert(rhs_bitmap.clone());
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() |= rhs_bitmap;
                }
            }
        }
    }

    /// Computes the Sub of `self` and a Hybrid `rhs`.
    pub fn sub_assign_hybrid(&mut self, rhs: &HybridBitmap) {
        let HybridBitmap::Large(tree) = rhs else {
            self.sub_assign(&Self::from_hybrid(rhs));
            return;
        };
        let mut rhs_bitmaps = tree.bitmaps().peekable();
        self.map.retain(|prefix, bitmap| {
            while rhs_bitmaps.peek().is_some_and(|&(p, _)| p < *prefix) {
                rhs_bitmaps.next();
            }
            match rhs_bitmaps.peek() {
                Some(&(p, rhs_bitmap)) if p == *prefix => {
                    *bitmap -= rhs_bitmap;
                    !bitmap.is_empty()
                }
                _ => true,
            }
        });
    }

    /// Computes the Xor of `self` and a Hybrid `rhs`.
    pub fn bitxor_assign_hybrid(&mut self, rhs: &HybridBitmap) {
        let HybridBitmap::Large(tree) = rhs else {
            self.bitxor_assign(&Self::from_hybrid(rhs));
            return;
        };
        for (prefix, rhs_bitmap) in tree.bitmaps() {
            match self.map.entry(prefix) {
                Entry::Vacant(entry) => {
                    entry.insert(rhs_bitmap.clone());
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() ^= rhs_bitmap;
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
            }
        }
    }

    fn apply_serialized(&mut self, operation: FastTreemapOp, buf: &[u8]) -> Result<()> {
        match parse_serialized_treemap(buf)? {
            SerializedTreemap::Empty => {
                if matches!(operation, FastTreemapOp::And) {
                    self.map.clear();
                }
                Ok(())
            }
            SerializedTreemap::Small(payload) => {
                let rhs = Self::from_sorted_values(&decode_small_values(payload)?);
                self.apply_fast(operation, &rhs);
                Ok(())
            }
            SerializedTreemap::Roaring(payload) => {
                self.apply_roaring_serialized(operation, payload)
            }
        }
    }

    fn apply_fast(&mut self, operation: FastTreemapOp, rhs: &Self) {
        match operation {
            FastTreemapOp::And => {
                self.map.retain(|prefix, bitmap| {
                    if let Some(rhs_bitmap) = rhs.map.get(prefix) {
                        *bitmap &= rhs_bitmap;
                        !bitmap.is_empty()
                    } else {
                        false
                    }
                });
            }
            FastTreemapOp::Or => {
                for (&prefix, rhs_bitmap) in &rhs.map {
                    match self.map.entry(prefix) {
                        Entry::Vacant(entry) => {
                            entry.insert(rhs_bitmap.clone());
                        }
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() |= rhs_bitmap;
                        }
                    }
                }
            }
            FastTreemapOp::Sub => {
                self.map.retain(|prefix, bitmap| {
                    if let Some(rhs_bitmap) = rhs.map.get(prefix) {
                        *bitmap -= rhs_bitmap;
                        !bitmap.is_empty()
                    } else {
                        true
                    }
                });
            }
            FastTreemapOp::Xor => {
                for (&prefix, rhs_bitmap) in &rhs.map {
                    match self.map.entry(prefix) {
                        Entry::Vacant(entry) => {
                            entry.insert(rhs_bitmap.clone());
                        }
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() ^= rhs_bitmap;
                            if entry.get().is_empty() {
                                entry.remove();
                            }
                        }
                    }
                }
            }
        }
    }

    /// Apply op with serialized roaring buf.
    ///
    /// # Performance
    ///
    /// And uses `intersection_with_serialized_unchecked`. The rhs stays
    /// serialized; only matching values are probed.
    /// Or, Sub, Xor deserialize each rhs bitmap, then apply the roaring
    /// operator. Upstream roaring has no `*_with_serialized_unchecked` for
    /// these ops. Implementing them requires our own RoaringBitmap. That
    /// complexity is not justified.
    /// A per-value variant (parse rhs, mutate lhs per value) measured
    /// 50-75x slower than deserialize-then-operate.
    fn apply_roaring_serialized(&mut self, operation: FastTreemapOp, buf: &[u8]) -> Result<()> {
        let reader = TreemapReader::new(buf).map_err(|error| {
            ErrorCode::BadBytes(format!("fail to decode roaring treemap: {error}"))
        })?;
        match operation {
            FastTreemapOp::And => {
                let mut lhs = mem::take(&mut self.map);
                let mut result = BTreeMap::new();
                for bitmap in reader.iter() {
                    let bitmap = bitmap.map_err(|error| {
                        ErrorCode::BadBytes(format!("fail to decode roaring treemap: {error}"))
                    })?;
                    while lhs
                        .keys()
                        .next()
                        .is_some_and(|prefix| *prefix < bitmap.prefix())
                    {
                        lhs.pop_first();
                    }
                    let Some(lhs_bitmap) = lhs.remove(&bitmap.prefix()) else {
                        continue;
                    };
                    let intersection = lhs_bitmap
                        .intersection_with_serialized_unchecked(Cursor::new(bitmap.bitmap_buf()))
                        .map_err(|error| {
                            ErrorCode::BadBytes(format!("fail to decode roaring bitmap: {error}"))
                        })?;
                    if !intersection.is_empty() {
                        result.insert(bitmap.prefix(), intersection);
                    }
                }
                self.map = result;
            }
            FastTreemapOp::Or => {
                for bitmap in reader.iter() {
                    let bitmap = bitmap.map_err(|error| {
                        ErrorCode::BadBytes(format!("fail to decode roaring treemap: {error}"))
                    })?;
                    let prefix = bitmap.prefix();
                    let rhs = RoaringBitmap::deserialize_unchecked_from(bitmap.bitmap_buf())
                        .map_err(|error| {
                            ErrorCode::BadBytes(format!("fail to decode roaring bitmap: {error}"))
                        })?;
                    if rhs.is_empty() {
                        continue;
                    }
                    match self.map.entry(prefix) {
                        Entry::Vacant(entry) => {
                            entry.insert(rhs);
                        }
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() |= rhs;
                        }
                    }
                }
            }
            FastTreemapOp::Xor => {
                for bitmap in reader.iter() {
                    let bitmap = bitmap.map_err(|error| {
                        ErrorCode::BadBytes(format!("fail to decode roaring treemap: {error}"))
                    })?;
                    let prefix = bitmap.prefix();
                    let rhs = RoaringBitmap::deserialize_unchecked_from(bitmap.bitmap_buf())
                        .map_err(|error| {
                            ErrorCode::BadBytes(format!("fail to decode roaring bitmap: {error}"))
                        })?;
                    if rhs.is_empty() {
                        continue;
                    }
                    match self.map.entry(prefix) {
                        Entry::Vacant(entry) => {
                            entry.insert(rhs);
                        }
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() ^= rhs;
                            if entry.get().is_empty() {
                                entry.remove();
                            }
                        }
                    }
                }
            }
            FastTreemapOp::Sub => {
                for bitmap in reader.iter() {
                    let bitmap = bitmap.map_err(|error| {
                        ErrorCode::BadBytes(format!("fail to decode roaring treemap: {error}"))
                    })?;
                    let prefix = bitmap.prefix();
                    if let Some(lhs_bitmap) = self.map.get_mut(&prefix) {
                        let rhs = RoaringBitmap::deserialize_unchecked_from(bitmap.bitmap_buf())
                            .map_err(|error| {
                                ErrorCode::BadBytes(format!(
                                    "fail to decode roaring bitmap: {error}"
                                ))
                            })?;
                        *lhs_bitmap -= &rhs;
                        if lhs_bitmap.is_empty() {
                            self.map.remove(&prefix);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl FastTreemap {
    fn from_roaring_tree(tree: &RoaringTreemap) -> Self {
        Self {
            map: tree
                .bitmaps()
                .map(|(prefix, bitmap)| (prefix, bitmap.clone()))
                .collect(),
        }
    }

    fn from_sorted_values(values: &[u64]) -> Self {
        Self {
            map: values
                .chunk_by(|a, b| *a >> 32 == *b >> 32)
                .map(|group| {
                    (
                        (group[0] >> 32) as u32,
                        RoaringBitmap::from_iter(group.iter().map(|value| *value as u32)),
                    )
                })
                .collect(),
        }
    }

    pub fn from_hybrid(hybrid: &HybridBitmap) -> Self {
        match hybrid {
            HybridBitmap::Large(tree) => Self::from_roaring_tree(tree),
            HybridBitmap::Small(small) => Self::from_sorted_values(small),
        }
    }
}

enum SerializedTreemap<'a> {
    Empty,
    Small(&'a [u8]),
    Roaring(&'a [u8]),
}

fn parse_serialized_treemap(buf: &[u8]) -> Result<SerializedTreemap<'_>> {
    if buf.is_empty() {
        return Ok(SerializedTreemap::Empty);
    }
    if buf.len() >= 2 && buf[..2] == HYBRID_MAGIC {
        if buf.len() < HYBRID_HEADER_LEN {
            return Err(ErrorCode::BadBytes("hybrid bitmap: buffer too short"));
        }
        if buf[2] != HYBRID_VERSION {
            return Err(ErrorCode::BadBytes(format!(
                "unsupported hybrid bitmap version: {}",
                buf[2]
            )));
        }
        return match buf[3] {
            HYBRID_KIND_SMALL => Ok(SerializedTreemap::Small(&buf[HYBRID_HEADER_LEN..])),
            HYBRID_KIND_LARGE => Ok(SerializedTreemap::Roaring(&buf[HYBRID_HEADER_LEN..])),
            kind => Err(ErrorCode::BadBytes(format!(
                "unknown hybrid bitmap kind: {kind}"
            ))),
        };
    }
    Ok(SerializedTreemap::Roaring(buf))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use roaring::RoaringTreemap;

    use super::FastTreemap;
    use super::FastTreemapOp;
    use crate::bitmap::HybridBitmap;
    use crate::bitmap::reader::create_format_spec_32_bitmap;
    use crate::bitmap::reader::create_format_spec_64_bitmap;
    use crate::bitmap::reader::create_format_spec_portable_64_bitmap;

    fn roaring_treemap(values: impl IntoIterator<Item = u64>) -> RoaringTreemap {
        values.into_iter().collect()
    }

    fn fast_treemap(values: &[u64]) -> FastTreemap {
        // from_sorted_values requires strictly increasing input.
        // This check also rejects duplicates.
        assert!(
            values.windows(2).all(|window| window[0] < window[1]),
            "fast_treemap requires sorted unique input"
        );
        FastTreemap::from_sorted_values(values)
    }

    fn treemap_values(bitmap: &FastTreemap) -> Vec<u64> {
        bitmap
            .map
            .iter()
            .flat_map(|(&prefix, bitmap)| {
                bitmap
                    .iter()
                    .map(move |value| (u64::from(prefix) << 32) | u64::from(value))
            })
            .collect()
    }

    fn serialized_hybrid(values: impl IntoIterator<Item = u64>) -> Vec<u8> {
        let bitmap = HybridBitmap::from_iter(values);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    fn serialized_legacy(values: impl IntoIterator<Item = u64>) -> Vec<u8> {
        let bitmap = roaring_treemap(values);
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    struct BitmapOperand {
        name: &'static str,
        values: Vec<u64>,
    }

    fn values_in_prefix(prefix: u64, range: RangeInclusive<u64>) -> Vec<u64> {
        range.map(|low| (prefix << 32) | low).collect()
    }

    fn bitmap_operands() -> Vec<BitmapOperand> {
        vec![
            BitmapOperand {
                name: "empty",
                values: vec![],
            },
            BitmapOperand {
                name: "single",
                values: vec![5],
            },
            BitmapOperand {
                name: "small_32",
                values: values_in_prefix(0, 0..=31),
            },
            BitmapOperand {
                name: "large_33",
                values: values_in_prefix(0, 0..=32),
            },
            BitmapOperand {
                name: "large_array_4096",
                values: values_in_prefix(0, 0..=4095),
            },
            BitmapOperand {
                name: "large_bitmap_4097",
                values: values_in_prefix(0, 0..=4096),
            },
            BitmapOperand {
                name: "large_full_65536",
                values: values_in_prefix(0, 0..=65535),
            },
            BitmapOperand {
                name: "multi_large_mixed",
                values: values_in_prefix(0, 0..=4096)
                    .into_iter()
                    .chain(values_in_prefix(1, 0..=31))
                    .collect(),
            },
            BitmapOperand {
                name: "prefix_max",
                values: vec![
                    u64::from(u32::MAX) << 32,
                    (u64::from(u32::MAX) << 32) + 9,
                    u64::MAX,
                ],
            },
        ]
    }

    #[test]
    fn test_fast_treemap_from_hybrid_values_equal() {
        for operand in bitmap_operands() {
            let hybrid = HybridBitmap::from_iter(operand.values.iter().copied());
            let fast = FastTreemap::from_hybrid(&hybrid);
            assert_eq!(treemap_values(&fast), operand.values, "{}", operand.name);
        }
    }

    #[test]
    fn test_fast_treemap_from_sorted_values_equal() {
        for operand in bitmap_operands() {
            let fast = fast_treemap(&operand.values);
            assert_eq!(treemap_values(&fast), operand.values, "{}", operand.name);
        }
    }

    #[test]
    fn test_fast_treemap_assign_serialized_values_equal() {
        let operands = bitmap_operands();
        for lhs in &operands {
            for rhs in &operands {
                let lhs_tree = roaring_treemap(lhs.values.iter().copied());
                let rhs_tree = roaring_treemap(rhs.values.iter().copied());
                let serialized = [
                    ("hybrid", serialized_hybrid(rhs.values.iter().copied())),
                    ("legacy", serialized_legacy(rhs.values.iter().copied())),
                ];
                for op in [
                    FastTreemapOp::And,
                    FastTreemapOp::Or,
                    FastTreemapOp::Sub,
                    FastTreemapOp::Xor,
                ] {
                    let expected = match op {
                        FastTreemapOp::And => (&lhs_tree & &rhs_tree).iter().collect::<Vec<_>>(),
                        FastTreemapOp::Or => (&lhs_tree | &rhs_tree).iter().collect::<Vec<_>>(),
                        FastTreemapOp::Sub => (&lhs_tree - &rhs_tree).iter().collect::<Vec<_>>(),
                        FastTreemapOp::Xor => (&lhs_tree ^ &rhs_tree).iter().collect::<Vec<_>>(),
                    };
                    for (fmt, rhs_bytes) in &serialized {
                        let mut fast = fast_treemap(&lhs.values);
                        match op {
                            FastTreemapOp::And => fast.bitand_assign_serialized(rhs_bytes).unwrap(),
                            FastTreemapOp::Or => fast.bitor_assign_serialized(rhs_bytes).unwrap(),
                            FastTreemapOp::Sub => fast.sub_assign_serialized(rhs_bytes).unwrap(),
                            FastTreemapOp::Xor => fast.bitxor_assign_serialized(rhs_bytes).unwrap(),
                        }
                        assert_eq!(
                            treemap_values(&fast),
                            expected,
                            "{op:?} {} {} {}",
                            lhs.name,
                            rhs.name,
                            fmt
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_fast_treemap_assign_hybrid_values_equal() {
        let operands = bitmap_operands();
        for lhs in &operands {
            for rhs in &operands {
                let lhs_roaring = roaring_treemap(lhs.values.iter().copied());
                let rhs_roaring = roaring_treemap(rhs.values.iter().copied());
                // from_iter keeps a 32-value rhs Small and makes a 33-value rhs Large,
                // so the matrix covers both branches of each *_assign_hybrid method.
                let rhs_hybrid = HybridBitmap::from_iter(rhs.values.iter().copied());
                for op in [
                    FastTreemapOp::And,
                    FastTreemapOp::Or,
                    FastTreemapOp::Sub,
                    FastTreemapOp::Xor,
                ] {
                    let expected = match op {
                        FastTreemapOp::And => {
                            (&lhs_roaring & &rhs_roaring).iter().collect::<Vec<_>>()
                        }
                        FastTreemapOp::Or => {
                            (&lhs_roaring | &rhs_roaring).iter().collect::<Vec<_>>()
                        }
                        FastTreemapOp::Sub => {
                            (&lhs_roaring - &rhs_roaring).iter().collect::<Vec<_>>()
                        }
                        FastTreemapOp::Xor => {
                            (&lhs_roaring ^ &rhs_roaring).iter().collect::<Vec<_>>()
                        }
                    };
                    let mut fast = fast_treemap(&lhs.values);
                    match op {
                        FastTreemapOp::And => fast.bitand_assign_hybrid(&rhs_hybrid),
                        FastTreemapOp::Or => fast.bitor_assign_hybrid(&rhs_hybrid),
                        FastTreemapOp::Sub => fast.sub_assign_hybrid(&rhs_hybrid),
                        FastTreemapOp::Xor => fast.bitxor_assign_hybrid(&rhs_hybrid),
                    }
                    assert_eq!(
                        treemap_values(&fast),
                        expected,
                        "{op:?} {} {}",
                        lhs.name,
                        rhs.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_fast_treemap_assign_zero_length_serialized_values_equal() {
        // The empty rhs in the matrix test above still carries a header.
        // This test covers input without any bytes.
        for operand in bitmap_operands() {
            for op in [
                FastTreemapOp::And,
                FastTreemapOp::Or,
                FastTreemapOp::Sub,
                FastTreemapOp::Xor,
            ] {
                let expected = match op {
                    FastTreemapOp::And => Vec::new(),
                    FastTreemapOp::Or => operand.values.clone(),
                    FastTreemapOp::Sub => operand.values.clone(),
                    FastTreemapOp::Xor => operand.values.clone(),
                };
                let mut fast = fast_treemap(&operand.values);
                match op {
                    FastTreemapOp::And => fast.bitand_assign_serialized(&[]).unwrap(),
                    FastTreemapOp::Or => fast.bitor_assign_serialized(&[]).unwrap(),
                    FastTreemapOp::Sub => fast.sub_assign_serialized(&[]).unwrap(),
                    FastTreemapOp::Xor => fast.bitxor_assign_serialized(&[]).unwrap(),
                }
                assert_eq!(
                    treemap_values(&fast),
                    expected,
                    "{op:?} zero-length {}",
                    operand.name
                );
            }
        }

        // deserialize of zero-length input returns an empty map.
        assert!(FastTreemap::deserialize(&[]).unwrap().is_empty());
    }

    #[test]
    fn test_fast_treemap_serialized_bytes_equal_hybrid() {
        for operand in bitmap_operands() {
            let mut hybrid = HybridBitmap::new();
            let mut fast = FastTreemap::new();
            for &value in &operand.values {
                hybrid.insert(value);
                fast.insert(value);
            }

            let mut hybrid_bytes = Vec::new();
            hybrid.serialize_into(&mut hybrid_bytes).unwrap();
            let mut fast_bytes = Vec::new();
            fast.serialize_into(&mut fast_bytes).unwrap();

            assert_eq!(fast_bytes, hybrid_bytes, "{}", operand.name);
        }
    }

    #[test]
    fn test_fast_treemap_serialized_values_equal_roaring() {
        let fixtures = [
            ("spec_32", create_format_spec_32_bitmap()),
            ("spec_64", create_format_spec_64_bitmap()),
            ("spec_portable_64", create_format_spec_portable_64_bitmap()),
        ];
        for (name, tree) in &fixtures {
            assert!(tree.len() > 32, "{name} fixture must stay large");

            let mut hybrid = Vec::new();
            hybrid.extend_from_slice(&crate::bitmap::HYBRID_MAGIC);
            hybrid.push(crate::bitmap::HYBRID_VERSION);
            hybrid.push(crate::bitmap::HYBRID_KIND_LARGE);
            tree.serialize_into(&mut hybrid).unwrap();

            let fast = FastTreemap::deserialize(&hybrid).unwrap();
            let mut fast_serialized = Vec::new();
            fast.serialize_into(&mut fast_serialized).unwrap();

            assert_eq!(fast_serialized, hybrid, "{name} round-trip bytes");
            let roaring = RoaringTreemap::deserialize_unchecked_from(
                &fast_serialized[crate::bitmap::HYBRID_HEADER_LEN..],
            )
            .unwrap();
            assert_eq!(
                roaring.iter().collect::<Vec<_>>(),
                tree.iter().collect::<Vec<_>>(),
                "{name} values equal roaring"
            );
        }
    }

    #[test]
    fn test_fast_treemap_wrappers_reject_malformed_payloads() {
        // The count header claims one prefix, but the bitmap body is missing.
        let mut truncated_treemap = 1_u64.to_le_bytes().to_vec();
        truncated_treemap.extend_from_slice(&0_u32.to_le_bytes());
        let bad_hybrid_version = [b'H', b'B', 2, crate::bitmap::HYBRID_KIND_LARGE];
        let bad_hybrid_kind = [b'H', b'B', 1, 9];

        let lhs_values = &[1, 1_u64 << 32];
        for (name, rhs) in [
            ("truncated treemap", truncated_treemap.as_slice()),
            ("bad hybrid version", bad_hybrid_version.as_slice()),
            ("bad hybrid kind", bad_hybrid_kind.as_slice()),
        ] {
            for op in [
                FastTreemapOp::And,
                FastTreemapOp::Or,
                FastTreemapOp::Sub,
                FastTreemapOp::Xor,
            ] {
                let mut bitmap = fast_treemap(lhs_values);
                let result = match op {
                    FastTreemapOp::And => bitmap.bitand_assign_serialized(rhs),
                    FastTreemapOp::Or => bitmap.bitor_assign_serialized(rhs),
                    FastTreemapOp::Sub => bitmap.sub_assign_serialized(rhs),
                    FastTreemapOp::Xor => bitmap.bitxor_assign_serialized(rhs),
                };
                assert!(result.is_err(), "{op:?} rejects {name}");
            }
        }
    }
}

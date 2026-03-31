// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashSet, ops::Range, sync::Arc};

use arrow_array::BooleanArray;
use deepsize::{Context, DeepSizeOf};
use roaring::RoaringBitmap;

/// Threshold for when a DeletionVector::Set should be promoted to a DeletionVector::Bitmap.
const BITMAP_THRESDHOLD: usize = 5_000;
// TODO: Benchmark to find a better value.

/// Represents a set of deleted row offsets in a single fragment.
#[derive(Debug, Clone)]
pub enum DeletionVector {
    NoDeletions,
    Set(HashSet<u32>),
    Bitmap(RoaringBitmap),
}

impl DeepSizeOf for DeletionVector {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            Self::NoDeletions => 0,
            Self::Set(set) => set.deep_size_of_children(context),
            // Inexact but probably close enough
            Self::Bitmap(bitmap) => bitmap.serialized_size(),
        }
    }
}

impl DeletionVector {
    #[allow(dead_code)] // Used in tests
    pub fn len(&self) -> usize {
        match self {
            Self::NoDeletions => 0,
            Self::Set(set) => set.len(),
            Self::Bitmap(bitmap) => bitmap.len() as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, i: u32) -> bool {
        match self {
            Self::NoDeletions => false,
            Self::Set(set) => set.contains(&i),
            Self::Bitmap(bitmap) => bitmap.contains(i),
        }
    }

    pub fn contains_range(&self, mut range: Range<u32>) -> bool {
        match self {
            Self::NoDeletions => range.is_empty(),
            Self::Set(set) => range.all(|i| set.contains(&i)),
            Self::Bitmap(bitmap) => bitmap.contains_range(range),
        }
    }

    fn range_cardinality(&self, range: Range<u32>) -> u64 {
        match self {
            Self::NoDeletions => 0,
            Self::Set(set) => range.fold(0, |acc, i| acc + set.contains(&i) as u64),
            Self::Bitmap(bitmap) => bitmap.range_cardinality(range),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u32> + Send + '_> {
        match self {
            Self::NoDeletions => Box::new(std::iter::empty()),
            Self::Set(set) => Box::new(set.iter().copied()),
            Self::Bitmap(bitmap) => Box::new(bitmap.iter()),
        }
    }

    pub fn into_sorted_iter(self) -> Box<dyn Iterator<Item = u32> + Send + 'static> {
        match self {
            Self::NoDeletions => Box::new(std::iter::empty()),
            Self::Set(set) => {
                // If we're using a set we shouldn't have too many values
                // and so this conversion should be affordable.
                let mut values = Vec::from_iter(set);
                values.sort();
                Box::new(values.into_iter())
            }
            // Bitmaps always iterate in sorted order
            Self::Bitmap(bitmap) => Box::new(bitmap.into_iter()),
        }
    }

    /// Create an iterator that iterates over the values in the deletion vector in sorted order.
    pub fn to_sorted_iter<'a>(&'a self) -> Box<dyn Iterator<Item = u32> + Send + 'a> {
        match self {
            Self::NoDeletions => Box::new(std::iter::empty()),
            // We have to make a clone when we're using a set
            // but sets should be relatively small.
            Self::Set(_) => self.clone().into_sorted_iter(),
            Self::Bitmap(bitmap) => Box::new(bitmap.iter()),
        }
    }

    // Note: deletion vectors are based on 32-bit offsets.  However, this function works
    // even when given 64-bit row addresses.  That is because `id as u32` returns the lower
    // 32 bits (the row offset) and the upper 32 bits are ignored.
    pub fn build_predicate(&self, row_addrs: std::slice::Iter<u64>) -> Option<BooleanArray> {
        match self {
            Self::Bitmap(bitmap) => Some(
                row_addrs
                    .map(|&id| !bitmap.contains(id as u32))
                    .collect::<Vec<_>>(),
            ),
            Self::Set(set) => Some(
                row_addrs
                    .map(|&id| !set.contains(&(id as u32)))
                    .collect::<Vec<_>>(),
            ),
            Self::NoDeletions => None,
        }
        .map(BooleanArray::from)
    }
}

/// Maps a naive offset into a fragment to the local row offset that is
/// not deleted.
///
/// For example, if the deletion vector is [0, 1, 2], then the mapping
/// would be:
///
/// - 0 -> 3
/// - 1 -> 4
/// - 2 -> 5
///
/// and so on.
///
/// This expects a monotonically increasing sequence of input offsets. State
/// is re-used between calls to `map_offset` to make the mapping more efficient.
pub struct OffsetMapper {
    dv: Arc<DeletionVector>,
    left: u32,
    last_diff: u32,
}

impl OffsetMapper {
    pub fn new(dv: Arc<DeletionVector>) -> Self {
        Self {
            dv,
            left: 0,
            last_diff: 0,
        }
    }

    pub fn map_offset(&mut self, offset: u32) -> u32 {
        // The best initial guess is the offset + last diff. That's the right
        // answer if there are no deletions in the range between the last
        // offset and the current one.
        let mut mid = offset + self.last_diff;
        let mut right = offset + self.dv.len() as u32;
        loop {
            let deleted_in_range = self.dv.range_cardinality(0..(mid + 1)) as u32;
            match mid.cmp(&(offset + deleted_in_range)) {
                std::cmp::Ordering::Equal if !self.dv.contains(mid) => {
                    self.last_diff = mid - offset;
                    return mid;
                }
                std::cmp::Ordering::Less => {
                    assert_ne!(self.left, mid + 1);
                    self.left = mid + 1;
                    mid = self.left + (right - self.left) / 2;
                }
                // There are cases where the mid is deleted but also equal in
                // comparison. For those we need to find a lower value.
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                    right = mid;
                    mid = self.left + (right - self.left) / 2;
                }
            }
        }
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::NoDeletions
    }
}

impl From<&DeletionVector> for RoaringBitmap {
    fn from(value: &DeletionVector) -> Self {
        match value {
            DeletionVector::Bitmap(bitmap) => bitmap.clone(),
            DeletionVector::Set(set) => Self::from_iter(set.iter()),
            DeletionVector::NoDeletions => Self::new(),
        }
    }
}

impl PartialEq for DeletionVector {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NoDeletions, Self::NoDeletions) => true,
            (Self::Set(set1), Self::Set(set2)) => set1 == set2,
            (Self::Bitmap(bitmap1), Self::Bitmap(bitmap2)) => bitmap1 == bitmap2,
            (Self::Set(set), Self::Bitmap(bitmap)) | (Self::Bitmap(bitmap), Self::Set(set)) => {
                let set = set.iter().copied().collect::<RoaringBitmap>();
                set == *bitmap
            }
            _ => false,
        }
    }
}

impl Extend<u32> for DeletionVector {
    fn extend<T: IntoIterator<Item = u32>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        // The mem::replace allows changing the variant of Self when we only
        // have &mut Self.
        *self = match (std::mem::take(self), iter.size_hint()) {
            (Self::NoDeletions, (_, Some(0))) => Self::NoDeletions,
            (Self::NoDeletions, (lower, _)) if lower >= BITMAP_THRESDHOLD => {
                let bitmap = iter.collect::<RoaringBitmap>();
                Self::Bitmap(bitmap)
            }
            (Self::NoDeletions, (_, Some(upper))) if upper < BITMAP_THRESDHOLD => {
                let set = iter.collect::<HashSet<_>>();
                Self::Set(set)
            }
            (Self::NoDeletions, _) => {
                // We don't know the size, so just try as a set and move to bitmap
                // if it ends up being big.
                let set = iter.collect::<HashSet<_>>();
                if set.len() > BITMAP_THRESDHOLD {
                    let bitmap = set.into_iter().collect::<RoaringBitmap>();
                    Self::Bitmap(bitmap)
                } else {
                    Self::Set(set)
                }
            }
            (Self::Set(mut set), _) => {
                set.extend(iter);
                if set.len() > BITMAP_THRESDHOLD {
                    let bitmap = set.drain().collect::<RoaringBitmap>();
                    Self::Bitmap(bitmap)
                } else {
                    Self::Set(set)
                }
            }
            (Self::Bitmap(mut bitmap), _) => {
                bitmap.extend(iter);
                Self::Bitmap(bitmap)
            }
        };
    }
}

// TODO: impl methods for DeletionVector
/// impl DeletionVector {
///     pub fn get(i: u32) -> bool { ... }
/// }
/// impl BitAnd for DeletionVector { ... }
impl IntoIterator for DeletionVector {
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send>;
    type Item = u32;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::NoDeletions => Box::new(std::iter::empty()),
            Self::Set(set) => {
                // In many cases, it's much better if this is sorted. It's
                // guaranteed to be small, so the cost is low.
                let mut sorted = set.into_iter().collect::<Vec<_>>();
                sorted.sort();
                Box::new(sorted.into_iter())
            }
            Self::Bitmap(bitmap) => Box::new(bitmap.into_iter()),
        }
    }
}

impl FromIterator<u32> for DeletionVector {
    fn from_iter<T: IntoIterator<Item = u32>>(iter: T) -> Self {
        let mut deletion_vector = Self::default();
        deletion_vector.extend(iter);
        deletion_vector
    }
}

impl From<RoaringBitmap> for DeletionVector {
    fn from(bitmap: RoaringBitmap) -> Self {
        if bitmap.is_empty() {
            Self::NoDeletions
        } else {
            Self::Bitmap(bitmap)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deletion_vector() {
        let set = HashSet::from_iter(0..100);
        let bitmap = RoaringBitmap::from_iter(0..100);

        let set_dv = DeletionVector::Set(set);
        let bitmap_dv = DeletionVector::Bitmap(bitmap);

        assert_eq!(set_dv, bitmap_dv);
    }

    #[test]
    fn test_threshold() {
        let dv = DeletionVector::from_iter(0..(BITMAP_THRESDHOLD as u32));
        assert!(matches!(dv, DeletionVector::Bitmap(_)));
    }

    #[test]
    fn test_map_offsets() {
        let dv = DeletionVector::from_iter(vec![3, 5]);
        let mut mapper = OffsetMapper::new(Arc::new(dv));

        let offsets = [0, 1, 2, 3, 4, 5, 6];
        let mut output = Vec::new();
        for offset in offsets.iter() {
            output.push(mapper.map_offset(*offset));
        }
        assert_eq!(output, vec![0, 1, 2, 4, 6, 7, 8]);

        let dv = DeletionVector::from_iter(vec![0, 1, 2]);
        let mut mapper = OffsetMapper::new(Arc::new(dv));

        let offsets = [0, 1, 2, 3, 4, 5, 6];

        let mut output = Vec::new();
        for offset in offsets.iter() {
            output.push(mapper.map_offset(*offset));
        }
        assert_eq!(output, vec![3, 4, 5, 6, 7, 8, 9]);
    }
}

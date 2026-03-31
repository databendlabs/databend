// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::io::Write;
use std::iter;
use std::ops::{Range, RangeBounds};
use std::{collections::BTreeMap, io::Read};

use arrow_array::{Array, BinaryArray, GenericBinaryArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use byteorder::{ReadBytesExt, WriteBytesExt};
use deepsize::DeepSizeOf;
use roaring::{MultiOps, RoaringBitmap, RoaringTreemap};

use crate::Result;

use super::address::RowAddress;

/// A row id mask to select or deselect particular row ids
///
/// If both the allow_list and the block_list are Some then the only selected
/// row ids are those that are in the allow_list but not in the block_list
/// (the block_list takes precedence)
///
/// If both the allow_list and the block_list are None (the default) then
/// all row ids are selected
#[derive(Clone, Debug, Default, DeepSizeOf)]
pub struct RowIdMask {
    /// If Some then only these row ids are selected
    pub allow_list: Option<RowIdTreeMap>,
    /// If Some then these row ids are not selected.
    pub block_list: Option<RowIdTreeMap>,
}

impl RowIdMask {
    // Create a mask allowing all rows, this is an alias for [default]
    pub fn all_rows() -> Self {
        Self::default()
    }

    // Create a mask that doesn't allow anything
    pub fn allow_nothing() -> Self {
        Self {
            allow_list: Some(RowIdTreeMap::new()),
            block_list: None,
        }
    }

    // Create a mask from an allow list
    pub fn from_allowed(allow_list: RowIdTreeMap) -> Self {
        Self {
            allow_list: Some(allow_list),
            block_list: None,
        }
    }

    // Create a mask from a block list
    pub fn from_block(block_list: RowIdTreeMap) -> Self {
        Self {
            allow_list: None,
            block_list: Some(block_list),
        }
    }

    // If there is both a block list and an allow list then collapse into just an allow list
    pub fn normalize(self) -> Self {
        if let Self {
            allow_list: Some(mut allow_list),
            block_list: Some(block_list),
        } = self
        {
            allow_list -= &block_list;
            Self {
                allow_list: Some(allow_list),
                block_list: None,
            }
        } else {
            self
        }
    }

    /// True if the row_id is selected by the mask, false otherwise
    pub fn selected(&self, row_id: u64) -> bool {
        match (&self.allow_list, &self.block_list) {
            (None, None) => true,
            (Some(allow_list), None) => allow_list.contains(row_id),
            (None, Some(block_list)) => !block_list.contains(row_id),
            (Some(allow_list), Some(block_list)) => {
                allow_list.contains(row_id) && !block_list.contains(row_id)
            }
        }
    }

    /// Return the indices of the input row ids that were valid
    pub fn selected_indices<'a>(&self, row_ids: impl Iterator<Item = &'a u64> + 'a) -> Vec<u64> {
        let enumerated_ids = row_ids.enumerate();
        match (&self.block_list, &self.allow_list) {
            (Some(block_list), Some(allow_list)) => {
                // Only take rows that are both in the allow list and not in the block list
                enumerated_ids
                    .filter(|(_, row_id)| {
                        !block_list.contains(**row_id) && allow_list.contains(**row_id)
                    })
                    .map(|(idx, _)| idx as u64)
                    .collect()
            }
            (Some(block_list), None) => {
                // Take rows that are not in the block list
                enumerated_ids
                    .filter(|(_, row_id)| !block_list.contains(**row_id))
                    .map(|(idx, _)| idx as u64)
                    .collect()
            }
            (None, Some(allow_list)) => {
                // Take rows that are in the allow list
                enumerated_ids
                    .filter(|(_, row_id)| allow_list.contains(**row_id))
                    .map(|(idx, _)| idx as u64)
                    .collect()
            }
            (None, None) => {
                // We should not encounter this case because callers should
                // check is_empty first.
                panic!("selected_indices called but prefilter has nothing to filter with")
            }
        }
    }

    /// Also block the given ids
    pub fn also_block(self, block_list: RowIdTreeMap) -> Self {
        if block_list.is_empty() {
            return self;
        }
        if let Some(existing) = self.block_list {
            Self {
                block_list: Some(existing | block_list),
                allow_list: self.allow_list,
            }
        } else {
            Self {
                block_list: Some(block_list),
                allow_list: self.allow_list,
            }
        }
    }

    /// Also allow the given ids
    pub fn also_allow(self, allow_list: RowIdTreeMap) -> Self {
        if let Some(existing) = self.allow_list {
            Self {
                block_list: self.block_list,
                allow_list: Some(existing | allow_list),
            }
        } else {
            Self {
                block_list: self.block_list,
                // allow_list = None means "all rows allowed" and so allowing
                //              more rows is meaningless
                allow_list: None,
            }
        }
    }

    /// Convert a mask into an arrow array
    ///
    /// A row id mask is not very arrow-compatible.  We can't make it a batch with
    /// two columns because the block list and allow list will have different lengths.  Also,
    /// there is no Arrow type for compressed bitmaps.
    ///
    /// However, we need to shove it into some kind of Arrow container to pass it along the
    /// datafusion stream.  Perhaps, in the future, we can add row id masks as first class
    /// types in datafusion, and this can be passed along as a mask / selection vector.
    ///
    /// We serialize this as a variable length binary array with two items.  The first item
    /// is the block list and the second item is the allow list.
    pub fn into_arrow(&self) -> Result<BinaryArray> {
        let block_list_length = self
            .block_list
            .as_ref()
            .map(|bl| bl.serialized_size())
            .unwrap_or(0);
        let allow_list_length = self
            .allow_list
            .as_ref()
            .map(|al| al.serialized_size())
            .unwrap_or(0);
        let lengths = vec![block_list_length, allow_list_length];
        let offsets = OffsetBuffer::from_lengths(lengths);
        let mut value_bytes = vec![0; block_list_length + allow_list_length];
        let mut validity = vec![false, false];
        if let Some(block_list) = &self.block_list {
            validity[0] = true;
            block_list.serialize_into(&mut value_bytes[0..])?;
        }
        if let Some(allow_list) = &self.allow_list {
            validity[1] = true;
            allow_list.serialize_into(&mut value_bytes[block_list_length..])?;
        }
        let values = Buffer::from(value_bytes);
        let nulls = NullBuffer::from(validity);
        Ok(BinaryArray::try_new(offsets, values, Some(nulls))?)
    }

    /// Deserialize a row id mask from Arrow
    pub fn from_arrow(array: &GenericBinaryArray<i32>) -> Result<Self> {
        let block_list = if array.is_null(0) {
            None
        } else {
            Some(RowIdTreeMap::deserialize_from(array.value(0)))
        }
        .transpose()?;

        let allow_list = if array.is_null(1) {
            None
        } else {
            Some(RowIdTreeMap::deserialize_from(array.value(1)))
        }
        .transpose()?;
        Ok(Self {
            block_list,
            allow_list,
        })
    }

    /// Return the maximum number of row ids that could be selected by this mask
    ///
    /// Will be None if there is no allow list
    pub fn max_len(&self) -> Option<u64> {
        if let Some(allow_list) = &self.allow_list {
            // If there is a block list we could theoretically intersect the two
            // but it's not clear if that is worth the effort.  Feel free to add later.
            allow_list.len()
        } else {
            None
        }
    }

    /// Iterate over the row ids that are selected by the mask
    ///
    /// This is only possible if there is an allow list and neither the
    /// allow list nor the block list contain any "full fragment" blocks.
    ///
    /// TODO: We could probably still iterate efficiently even if the block
    /// list contains "full fragment" blocks but that would require some
    /// extra logic.
    pub fn iter_ids(&self) -> Option<Box<dyn Iterator<Item = RowAddress> + '_>> {
        if let Some(mut allow_iter) = self.allow_list.as_ref().and_then(|list| list.row_ids()) {
            if let Some(block_list) = &self.block_list {
                if let Some(block_iter) = block_list.row_ids() {
                    let mut block_iter = block_iter.peekable();
                    Some(Box::new(iter::from_fn(move || {
                        for allow_id in allow_iter.by_ref() {
                            while let Some(block_id) = block_iter.peek() {
                                if *block_id >= allow_id {
                                    break;
                                }
                                block_iter.next();
                            }
                            if let Some(block_id) = block_iter.peek() {
                                if *block_id == allow_id {
                                    continue;
                                }
                            }
                            return Some(allow_id);
                        }
                        None
                    })))
                } else {
                    // There is a block list but we can't iterate over it, give up
                    None
                }
            } else {
                // There is no block list, use the allow list
                Some(Box::new(allow_iter))
            }
        } else {
            None
        }
    }
}

impl std::ops::Not for RowIdMask {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self {
            block_list: self.allow_list,
            allow_list: self.block_list,
        }
    }
}

impl std::ops::BitAnd for RowIdMask {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        let block_list = match (self.block_list, rhs.block_list) {
            (None, None) => None,
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (Some(lhs), Some(rhs)) => Some(lhs | rhs),
        };
        let allow_list = match (self.allow_list, rhs.allow_list) {
            (None, None) => None,
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (Some(lhs), Some(rhs)) => Some(lhs & rhs),
        };
        Self {
            block_list,
            allow_list,
        }
    }
}

impl std::ops::BitOr for RowIdMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        let this = self.normalize();
        let rhs = rhs.normalize();
        let block_list = if let Some(mut self_block_list) = this.block_list {
            match (&rhs.allow_list, rhs.block_list) {
                // If RHS is allow all, then our block list disappears
                (None, None) => None,
                // If RHS is allow list, remove allowed from our block list
                (Some(allow_list), None) => {
                    self_block_list -= allow_list;
                    Some(self_block_list)
                }
                // If RHS is block list, intersect
                (None, Some(block_list)) => Some(self_block_list & block_list),
                // We normalized to avoid this path
                (Some(_), Some(_)) => unreachable!(),
            }
        } else if let Some(mut rhs_block_list) = rhs.block_list {
            if let Some(allow_list) = &this.allow_list {
                rhs_block_list -= allow_list;
                Some(rhs_block_list)
            } else {
                Some(rhs_block_list)
            }
        } else {
            None
        };

        let allow_list = match (this.allow_list, rhs.allow_list) {
            (None, None) => None,
            // Remember that an allow list of None means "all rows" and
            // so "all rows" | "some rows" is always "all rows"
            (Some(_), None) => None,
            (None, Some(_)) => None,
            (Some(lhs), Some(rhs)) => Some(lhs | rhs),
        };
        Self {
            block_list,
            allow_list,
        }
    }
}

/// A collection of row ids.
///
/// These row ids may either be stable-style (where they can be an incrementing
/// u64 sequence) or address style, where they are a fragment id and a row offset.
/// When address style, this supports setting entire fragments as selected,
/// without needing to enumerate all the ids in the fragment.
///
/// This is similar to a [RoaringTreemap] but it is optimized for the case where
/// entire fragments are selected or deselected.
#[derive(Clone, Debug, Default, PartialEq, DeepSizeOf)]
pub struct RowIdTreeMap {
    /// The contents of the set. If there is a pair (k, Full) then the entire
    /// fragment k is selected. If there is a pair (k, Partial(v)) then the
    /// fragment k has the selected rows in v.
    inner: BTreeMap<u32, RowAddrSelection>,
}

#[derive(Clone, Debug, PartialEq)]
enum RowAddrSelection {
    Full,
    Partial(RoaringBitmap),
}

impl DeepSizeOf for RowAddrSelection {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        match self {
            Self::Full => 0,
            Self::Partial(bitmap) => bitmap.serialized_size(),
        }
    }
}

impl RowAddrSelection {
    fn union_all(selections: &[&Self]) -> Self {
        let mut is_full = false;

        let res = Self::Partial(
            selections
                .iter()
                .filter_map(|selection| match selection {
                    Self::Full => {
                        is_full = true;
                        None
                    }
                    Self::Partial(bitmap) => Some(bitmap),
                })
                .union(),
        );

        if is_full {
            Self::Full
        } else {
            res
        }
    }
}

impl RowIdTreeMap {
    /// Create an empty set
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The number of rows in the map
    ///
    /// If there are any "full fragment" items then this is unknown and None is returned
    pub fn len(&self) -> Option<u64> {
        self.inner
            .values()
            .map(|row_addr_selection| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(indices) => Some(indices.len()),
            })
            .try_fold(0_u64, |acc, next| next.map(|next| next + acc))
    }

    /// An iterator of row ids
    ///
    /// If there are any "full fragment" items then this can't be calculated and None
    /// is returned
    pub fn row_ids(&self) -> Option<impl Iterator<Item = RowAddress> + '_> {
        let inner_iters = self
            .inner
            .iter()
            .filter_map(|(frag_id, row_addr_selection)| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(bitmap) => Some(
                    bitmap
                        .iter()
                        .map(|row_offset| RowAddress::new_from_parts(*frag_id, row_offset)),
                ),
            })
            .collect::<Vec<_>>();
        if inner_iters.len() != self.inner.len() {
            None
        } else {
            Some(inner_iters.into_iter().flatten())
        }
    }

    /// Insert a single value into the set
    ///
    /// Returns true if the value was not already in the set.
    ///
    /// ```rust
    /// use lance_core::utils::mask::RowIdTreeMap;
    ///
    /// let mut set = RowIdTreeMap::new();
    /// assert_eq!(set.insert(10), true);
    /// assert_eq!(set.insert(10), false);
    /// assert_eq!(set.contains(10), true);
    /// ```
    pub fn insert(&mut self, value: u64) -> bool {
        let fragment = (value >> 32) as u32;
        let row_addr = value as u32;
        match self.inner.get_mut(&fragment) {
            None => {
                let mut set = RoaringBitmap::new();
                set.insert(row_addr);
                self.inner.insert(fragment, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Full) => false,
            Some(RowAddrSelection::Partial(set)) => set.insert(row_addr),
        }
    }

    /// Insert a range of values into the set
    pub fn insert_range<R: RangeBounds<u64>>(&mut self, range: R) -> u64 {
        // Separate the start and end into high and low bits.
        let (mut start_high, mut start_low) = match range.start_bound() {
            std::ops::Bound::Included(&start) => ((start >> 32) as u32, start as u32),
            std::ops::Bound::Excluded(&start) => {
                let start = start.saturating_add(1);
                ((start >> 32) as u32, start as u32)
            }
            std::ops::Bound::Unbounded => (0, 0),
        };

        let (end_high, end_low) = match range.end_bound() {
            std::ops::Bound::Included(&end) => ((end >> 32) as u32, end as u32),
            std::ops::Bound::Excluded(&end) => {
                let end = end.saturating_sub(1);
                ((end >> 32) as u32, end as u32)
            }
            std::ops::Bound::Unbounded => (u32::MAX, u32::MAX),
        };

        let mut count = 0;

        while start_high <= end_high {
            let start = start_low;
            let end = if start_high == end_high {
                end_low
            } else {
                u32::MAX
            };
            let fragment = start_high;
            match self.inner.get_mut(&fragment) {
                None => {
                    let mut set = RoaringBitmap::new();
                    count += set.insert_range(start..=end);
                    self.inner.insert(fragment, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {}
                Some(RowAddrSelection::Partial(set)) => {
                    count += set.insert_range(start..=end);
                }
            }
            start_high += 1;
            start_low = 0;
        }

        count
    }

    /// Add a bitmap for a single fragment
    pub fn insert_bitmap(&mut self, fragment: u32, bitmap: RoaringBitmap) {
        self.inner
            .insert(fragment, RowAddrSelection::Partial(bitmap));
    }

    /// Add a whole fragment to the set
    pub fn insert_fragment(&mut self, fragment_id: u32) {
        self.inner.insert(fragment_id, RowAddrSelection::Full);
    }

    pub fn get_fragment_bitmap(&self, fragment_id: u32) -> Option<&RoaringBitmap> {
        match self.inner.get(&fragment_id) {
            None => None,
            Some(RowAddrSelection::Full) => None,
            Some(RowAddrSelection::Partial(set)) => Some(set),
        }
    }

    /// Returns whether the set contains the given value
    pub fn contains(&self, value: u64) -> bool {
        let upper = (value >> 32) as u32;
        let lower = value as u32;
        match self.inner.get(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => true,
            Some(RowAddrSelection::Partial(fragment_set)) => fragment_set.contains(lower),
        }
    }

    pub fn remove(&mut self, value: u64) -> bool {
        let upper = (value >> 32) as u32;
        let lower = value as u32;
        match self.inner.get_mut(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => {
                let mut set = RoaringBitmap::full();
                set.remove(lower);
                self.inner.insert(upper, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Partial(lower_set)) => {
                let removed = lower_set.remove(lower);
                if lower_set.is_empty() {
                    self.inner.remove(&upper);
                }
                removed
            }
        }
    }

    pub fn retain_fragments(&mut self, frag_ids: impl IntoIterator<Item = u32>) {
        let frag_id_set = frag_ids.into_iter().collect::<HashSet<_>>();
        self.inner
            .retain(|frag_id, _| frag_id_set.contains(frag_id));
    }

    /// Compute the serialized size of the set.
    pub fn serialized_size(&self) -> usize {
        // Starts at 4 because of the u32 num_entries
        let mut size = 4;
        for set in self.inner.values() {
            // Each entry is 8 bytes for the fragment id and the bitmap size
            size += 8;
            if let RowAddrSelection::Partial(set) = set {
                size += set.serialized_size();
            }
        }
        size
    }

    /// Serialize the set into the given buffer
    ///
    /// The serialization format is stable and used for index serialization
    ///
    /// The serialization format is:
    /// * u32: num_entries
    ///
    /// for each entry:
    ///   * u32: fragment_id
    ///   * u32: bitmap size
    ///   * \[u8\]: bitmap
    ///
    /// If bitmap size is zero then the entire fragment is selected.
    pub fn serialize_into<W: Write>(&self, mut writer: W) -> Result<()> {
        writer.write_u32::<byteorder::LittleEndian>(self.inner.len() as u32)?;
        for (fragment, set) in &self.inner {
            writer.write_u32::<byteorder::LittleEndian>(*fragment)?;
            if let RowAddrSelection::Partial(set) = set {
                writer.write_u32::<byteorder::LittleEndian>(set.serialized_size() as u32)?;
                set.serialize_into(&mut writer)?;
            } else {
                writer.write_u32::<byteorder::LittleEndian>(0)?;
            }
        }
        Ok(())
    }

    /// Deserialize the set from the given buffer
    pub fn deserialize_from<R: Read>(mut reader: R) -> Result<Self> {
        let num_entries = reader.read_u32::<byteorder::LittleEndian>()?;
        let mut inner = BTreeMap::new();
        for _ in 0..num_entries {
            let fragment = reader.read_u32::<byteorder::LittleEndian>()?;
            let bitmap_size = reader.read_u32::<byteorder::LittleEndian>()?;
            if bitmap_size == 0 {
                inner.insert(fragment, RowAddrSelection::Full);
            } else {
                let mut buffer = vec![0; bitmap_size as usize];
                reader.read_exact(&mut buffer)?;
                let set = RoaringBitmap::deserialize_from(&buffer[..])?;
                inner.insert(fragment, RowAddrSelection::Partial(set));
            }
        }
        Ok(Self { inner })
    }

    pub fn union_all(maps: &[&Self]) -> Self {
        let mut new_map = BTreeMap::new();

        for map in maps {
            for (fragment, selection) in &map.inner {
                new_map
                    .entry(fragment)
                    // I hate this allocation, but I can't think of a better way
                    .or_insert_with(|| Vec::with_capacity(maps.len()))
                    .push(selection);
            }
        }

        let new_map = new_map
            .into_iter()
            .map(|(&fragment, selections)| (fragment, RowAddrSelection::union_all(&selections)))
            .collect();

        Self { inner: new_map }
    }

    /// Apply a mask to the row ids
    ///
    /// If there is an allow list then this will intersect the set with the allow list
    /// If there is a block list then this will subtract the block list from the set
    pub fn mask(&mut self, mask: &RowIdMask) {
        if let Some(allow_list) = &mask.allow_list {
            *self &= allow_list;
        }
        if let Some(block_list) = &mask.block_list {
            *self -= block_list;
        }
    }

    /// Convert the set into an iterator of row addrs
    ///
    /// # Safety
    ///
    /// This is unsafe because if any of the inner RowAddrSelection elements
    /// is not a Partial then the iterator will panic because we don't know
    /// the size of the bitmap.
    pub unsafe fn into_addr_iter(self) -> impl Iterator<Item = u64> {
        self.inner
            .into_iter()
            .flat_map(|(fragment, selection)| match selection {
                RowAddrSelection::Full => panic!("Size of full fragment is unknown"),
                RowAddrSelection::Partial(bitmap) => bitmap.into_iter().map(move |val| {
                    let fragment = fragment as u64;
                    let row_offset = val as u64;
                    (fragment << 32) | row_offset
                }),
            })
    }
}

impl std::ops::BitOr<Self> for RowIdTreeMap {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl std::ops::BitOrAssign<Self> for RowIdTreeMap {
    fn bitor_assign(&mut self, rhs: Self) {
        for (fragment, rhs_set) in &rhs.inner {
            let lhs_set = self.inner.get_mut(fragment);
            if let Some(lhs_set) = lhs_set {
                match lhs_set {
                    RowAddrSelection::Full => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    RowAddrSelection::Partial(lhs_bitmap) => match rhs_set {
                        RowAddrSelection::Full => {
                            *lhs_set = RowAddrSelection::Full;
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_bitmap |= rhs_set;
                        }
                    },
                }
            } else {
                self.inner.insert(*fragment, rhs_set.clone());
            }
        }
    }
}

impl std::ops::BitAnd<Self> for RowIdTreeMap {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self &= &rhs;
        self
    }
}

impl std::ops::BitAndAssign<&Self> for RowIdTreeMap {
    fn bitand_assign(&mut self, rhs: &Self) {
        // Remove fragment that aren't on the RHS
        self.inner
            .retain(|fragment, _| rhs.inner.contains_key(fragment));

        // For fragments that are on the RHS, intersect the bitmaps
        for (fragment, mut lhs_set) in &mut self.inner {
            match (&mut lhs_set, rhs.inner.get(fragment)) {
                (_, None) => {} // Already handled by retain
                (_, Some(RowAddrSelection::Full)) => {
                    // Everything selected on RHS, so can leave LHS untouched.
                }
                (RowAddrSelection::Partial(lhs_set), Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set &= rhs_set;
                }
                (RowAddrSelection::Full, Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set = RowAddrSelection::Partial(rhs_set.clone());
                }
            }
        }
        // Some bitmaps might now be empty. If they are, we should remove them.
        self.inner.retain(|_, set| match set {
            RowAddrSelection::Partial(set) => !set.is_empty(),
            RowAddrSelection::Full => true,
        });
    }
}

impl std::ops::Sub<Self> for RowIdTreeMap {
    type Output = Self;

    fn sub(mut self, rhs: Self) -> Self {
        self -= &rhs;
        self
    }
}

impl std::ops::SubAssign<&Self> for RowIdTreeMap {
    fn sub_assign(&mut self, rhs: &Self) {
        for (fragment, rhs_set) in &rhs.inner {
            match self.inner.get_mut(fragment) {
                None => {}
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                    match rhs_set {
                        RowAddrSelection::Full => {
                            self.inner.remove(fragment);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            // This generally won't be hit.
                            let mut set = RoaringBitmap::full();
                            set -= rhs_set;
                            self.inner.insert(*fragment, RowAddrSelection::Partial(set));
                        }
                    }
                }
                Some(RowAddrSelection::Partial(lhs_set)) => match rhs_set {
                    RowAddrSelection::Full => {
                        self.inner.remove(fragment);
                    }
                    RowAddrSelection::Partial(rhs_set) => {
                        *lhs_set -= rhs_set;
                        if lhs_set.is_empty() {
                            self.inner.remove(fragment);
                        }
                    }
                },
            }
        }
    }
}

impl FromIterator<u64> for RowIdTreeMap {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        let mut inner = BTreeMap::new();
        for row_id in iter {
            let upper = (row_id >> 32) as u32;
            let lower = row_id as u32;
            match inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
        Self { inner }
    }
}

impl<'a> FromIterator<&'a u64> for RowIdTreeMap {
    fn from_iter<T: IntoIterator<Item = &'a u64>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().copied())
    }
}

impl From<Range<u64>> for RowIdTreeMap {
    fn from(range: Range<u64>) -> Self {
        let mut map = Self::default();
        map.insert_range(range);
        map
    }
}

impl From<RoaringTreemap> for RowIdTreeMap {
    fn from(roaring: RoaringTreemap) -> Self {
        let mut inner = BTreeMap::new();
        for (fragment, set) in roaring.bitmaps() {
            inner.insert(fragment, RowAddrSelection::Partial(set.clone()));
        }
        Self { inner }
    }
}

impl Extend<u64> for RowIdTreeMap {
    fn extend<T: IntoIterator<Item = u64>>(&mut self, iter: T) {
        for row_id in iter {
            let upper = (row_id >> 32) as u32;
            let lower = row_id as u32;
            match self.inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    self.inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
    }
}

impl<'a> Extend<&'a u64> for RowIdTreeMap {
    fn extend<T: IntoIterator<Item = &'a u64>>(&mut self, iter: T) {
        self.extend(iter.into_iter().copied())
    }
}

// Extending with RowIdTreeMap is basically a cumulative set union
impl Extend<Self> for RowIdTreeMap {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        for other in iter {
            for (fragment, set) in other.inner {
                match self.inner.get_mut(&fragment) {
                    None => {
                        self.inner.insert(fragment, set);
                    }
                    Some(RowAddrSelection::Full) => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    Some(RowAddrSelection::Partial(lhs_set)) => match set {
                        RowAddrSelection::Full => {
                            self.inner.insert(fragment, RowAddrSelection::Full);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_set |= rhs_set;
                        }
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prop_assert_eq;

    #[test]
    fn test_ops() {
        let mask = RowIdMask::default();
        assert!(mask.selected(1));
        assert!(mask.selected(5));
        let block_list = mask.also_block(RowIdTreeMap::from_iter(&[0, 5, 15]));
        assert!(block_list.selected(1));
        assert!(!block_list.selected(5));
        let allow_list = RowIdMask::from_allowed(RowIdTreeMap::from_iter(&[0, 2, 5]));
        assert!(!allow_list.selected(1));
        assert!(allow_list.selected(5));
        let combined = block_list & allow_list;
        assert!(combined.selected(2));
        assert!(!combined.selected(0));
        assert!(!combined.selected(5));
        let other = RowIdMask::from_allowed(RowIdTreeMap::from_iter(&[3]));
        let combined = combined | other;
        assert!(combined.selected(2));
        assert!(combined.selected(3));
        assert!(!combined.selected(0));
        assert!(!combined.selected(5));

        let block_list = RowIdMask::from_block(RowIdTreeMap::from_iter(&[0]));
        let allow_list = RowIdMask::from_allowed(RowIdTreeMap::from_iter(&[3]));
        let combined = block_list | allow_list;
        assert!(combined.selected(1));
    }

    #[test]
    fn test_logical_or() {
        let allow1 = RowIdMask::from_allowed(RowIdTreeMap::from_iter(&[5, 6, 7, 8, 9]));
        let block1 = RowIdMask::from_block(RowIdTreeMap::from_iter(&[5, 6]));
        let mixed1 = allow1
            .clone()
            .also_block(block1.block_list.as_ref().unwrap().clone());
        let allow2 = RowIdMask::from_allowed(RowIdTreeMap::from_iter(&[2, 3, 4, 5, 6, 7, 8]));
        let block2 = RowIdMask::from_block(RowIdTreeMap::from_iter(&[4, 5]));
        let mixed2 = allow2
            .clone()
            .also_block(block2.block_list.as_ref().unwrap().clone());

        fn check(lhs: &RowIdMask, rhs: &RowIdMask, expected: &[u64]) {
            for mask in [lhs.clone() | rhs.clone(), rhs.clone() | lhs.clone()] {
                let values = (0..10)
                    .filter(|val| mask.selected(*val))
                    .collect::<Vec<_>>();
                assert_eq!(&values, expected);
            }
        }

        check(&allow1, &allow1, &[5, 6, 7, 8, 9]);
        check(&block1, &block1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&mixed1, &mixed1, &[7, 8, 9]);
        check(&allow2, &allow2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed2, &mixed2, &[2, 3, 6, 7, 8]);

        check(&allow1, &block1, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed1, &[5, 6, 7, 8, 9]);
        check(&allow1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &block2, &[0, 1, 2, 3, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed2, &[2, 3, 5, 6, 7, 8, 9]);
        check(&block1, &mixed1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&block1, &allow2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&block1, &block2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&block1, &mixed2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&mixed1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&mixed1, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed1, &mixed2, &[2, 3, 6, 7, 8, 9]);
        check(&allow2, &block2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow2, &mixed2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &mixed2, &[0, 1, 2, 3, 6, 7, 8, 9]);
    }

    #[test]
    fn test_map_insert_range() {
        let ranges = &[
            (0..10),
            (40..500),
            ((u32::MAX as u64 - 10)..(u32::MAX as u64 + 20)),
        ];

        for range in ranges {
            let mut mask = RowIdTreeMap::default();

            let count = mask.insert_range(range.clone());
            let expected = range.end - range.start;
            assert_eq!(count, expected);

            let count = mask.insert_range(range.clone());
            assert_eq!(count, 0);

            let new_range = range.start + 5..range.end + 5;
            let count = mask.insert_range(new_range.clone());
            assert_eq!(count, 5);
        }

        let mut mask = RowIdTreeMap::default();
        let count = mask.insert_range(..10);
        assert_eq!(count, 10);
        assert!(mask.contains(0));

        let count = mask.insert_range(20..=24);
        assert_eq!(count, 5);

        mask.insert_fragment(0);
        let count = mask.insert_range(100..200);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_map_remove() {
        let mut mask = RowIdTreeMap::default();

        assert!(!mask.remove(20));

        mask.insert(20);
        assert!(mask.contains(20));
        assert!(mask.remove(20));
        assert!(!mask.contains(20));

        mask.insert_range(10..=20);
        assert!(mask.contains(15));
        assert!(mask.remove(15));
        assert!(!mask.contains(15));

        // We don't test removing from a full fragment, because that would take
        // a lot of memory.
    }

    proptest::proptest! {
        #[test]
        fn test_map_serialization_roundtrip(
            values in proptest::collection::vec(
                (0..u32::MAX, proptest::option::of(proptest::collection::vec(0..u32::MAX, 0..1000))),
                0..10
            )
        ) {
            let mut mask = RowIdTreeMap::default();
            for (fragment, rows) in values {
                if let Some(rows) = rows {
                    let bitmap = RoaringBitmap::from_iter(rows);
                    mask.insert_bitmap(fragment, bitmap);
                } else {
                    mask.insert_fragment(fragment);
                }
            }

            let mut data = Vec::new();
            mask.serialize_into(&mut data).unwrap();
            let deserialized = RowIdTreeMap::deserialize_from(data.as_slice()).unwrap();
            prop_assert_eq!(mask, deserialized);
        }

        #[test]
        fn test_map_intersect(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowIdTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowIdTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowIdTreeMap::default();
            for fragment in &left_full_fragments {
                if right_full_fragments.contains(fragment) {
                    expected.insert_fragment(*fragment);
                }
            }

            let left_in_right = left_rows.iter().filter(|row| {
                right_rows.contains(row)
                    || right_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(left_in_right);
            let right_in_left = right_rows.iter().filter(|row| {
                left_rows.contains(row)
                    || left_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(right_in_left);

            let actual = left & right;
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_union(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowIdTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowIdTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowIdTreeMap::default();
            for fragment in left_full_fragments {
                expected.insert_fragment(fragment);
            }
            for fragment in right_full_fragments {
                expected.insert_fragment(fragment);
            }

            let combined_rows = left_rows.iter().chain(right_rows.iter());
            expected.extend(combined_rows);

            let actual = left | right;
            for actual_key_val in &actual.inner {
                proptest::prop_assert!(expected.inner.contains_key(actual_key_val.0));
                let expected_val = expected.inner.get(actual_key_val.0).unwrap();
                prop_assert_eq!(
                    actual_key_val.1,
                    expected_val,
                    "error on key {}",
                    actual_key_val.0
                );
            }
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_subassign_rows(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowIdTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowIdTreeMap::default();
            right.extend(right_rows.iter().copied());

            let mut expected = left.clone();
            for row in right_rows {
                expected.remove(row);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

        #[test]
        fn test_map_subassign_frags(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowIdTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowIdTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }

            let mut expected = left.clone();
            for fragment in right_full_fragments {
                expected.inner.remove(&fragment);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

    }

    #[test]
    fn test_iter_ids() {
        let mut mask = RowIdMask::default();
        assert!(mask.iter_ids().is_none());

        // Test with just an allow list
        let mut allow_list = RowIdTreeMap::default();
        allow_list.extend([1, 5, 10].iter().copied());
        mask.allow_list = Some(allow_list);

        let ids: Vec<_> = mask.iter_ids().unwrap().collect();
        assert_eq!(
            ids,
            vec![
                RowAddress::new_from_parts(0, 1),
                RowAddress::new_from_parts(0, 5),
                RowAddress::new_from_parts(0, 10)
            ]
        );

        // Test with both allow list and block list
        let mut block_list = RowIdTreeMap::default();
        block_list.extend([5].iter().copied());
        mask.block_list = Some(block_list);

        let ids: Vec<_> = mask.iter_ids().unwrap().collect();
        assert_eq!(
            ids,
            vec![
                RowAddress::new_from_parts(0, 1),
                RowAddress::new_from_parts(0, 10)
            ]
        );

        // Test with full fragment in block list
        let mut block_list = RowIdTreeMap::default();
        block_list.insert_fragment(0);
        mask.block_list = Some(block_list);
        assert!(mask.iter_ids().is_none());

        // Test with full fragment in allow list
        mask.block_list = None;
        let mut allow_list = RowIdTreeMap::default();
        allow_list.insert_fragment(0);
        mask.allow_list = Some(allow_list);
        assert!(mask.iter_ids().is_none());
    }
}

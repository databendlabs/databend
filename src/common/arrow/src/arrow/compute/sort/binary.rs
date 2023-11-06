use super::common;
use super::SortOptions;
use crate::arrow::array::BinaryArray;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::offset::Offset;
use crate::arrow::types::Index;

pub(super) fn indices_sorted_unstable_by<I: Index, O: Offset>(
    array: &BinaryArray<O>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let get = |idx| unsafe { array.value_unchecked(idx) };
    let cmp = |lhs: &&[u8], rhs: &&[u8]| lhs.cmp(rhs);
    common::indices_sorted_unstable_by(array.validity(), get, cmp, array.len(), options, limit)
}

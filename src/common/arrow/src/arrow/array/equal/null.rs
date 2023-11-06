use crate::arrow::array::Array;
use crate::arrow::array::NullArray;

#[inline]
pub(super) fn equal(lhs: &NullArray, rhs: &NullArray) -> bool {
    lhs.len() == rhs.len()
}

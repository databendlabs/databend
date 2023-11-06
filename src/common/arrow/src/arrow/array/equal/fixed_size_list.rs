use crate::arrow::array::Array;
use crate::arrow::array::FixedSizeListArray;

pub(super) fn equal(lhs: &FixedSizeListArray, rhs: &FixedSizeListArray) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}

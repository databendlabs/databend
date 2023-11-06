use crate::arrow::array::Array;
use crate::arrow::array::MapArray;

pub(super) fn equal(lhs: &MapArray, rhs: &MapArray) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}

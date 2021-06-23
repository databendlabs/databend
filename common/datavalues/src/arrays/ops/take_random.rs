use std::convert::TryFrom;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeListArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::UInt32Array;
use unsafe_unwrap::UnsafeUnwrap;

use crate::arrays::DataArrayBase;
use crate::arrays::DataArrayWrap;
use crate::DFNumericType;
use crate::DFStringArray;

/// Random access
pub trait TakeRandom {
    type Item;

    /// Get a nullable value by index.
    ///
    /// # Safety
    ///
    /// Out of bounds access doesn't Error but will return a Null value
    fn get(&self, index: usize) -> Option<Self::Item>;

    /// Get a value by index and ignore the null bit.
    ///
    /// # Safety
    ///
    /// This doesn't check if the underlying type is null or not and may return an uninitialized value.
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item;
}

// Utility trait because associated type needs a lifetime
pub trait TakeRandomUtf8 {
    type Item;

    /// Get a nullable value by index.
    ///
    /// # Safety
    ///
    /// Out of bounds access doesn't Error but will return a Null value
    fn get(self, index: usize) -> Option<Self::Item>;

    /// Get a value by index and ignore the null bit.
    ///
    /// # Safety
    /// This doesn't check if the underlying type is null or not and may return an uninitialized value.
    unsafe fn get_unchecked(self, index: usize) -> Self::Item;
}

pub enum TakeIdx<'a, I, INulls>
where
    I: Iterator<Item = usize>,
    INulls: Iterator<Item = Option<usize>>,
{
    Array(&'a UInt32Array),
    Iter(I),
    // will return a null where None
    IterNulls(INulls),
}

pub type Dummy<T> = std::iter::Once<T>;
pub type TakeIdxIter<'a, I> = TakeIdx<'a, I, Dummy<Option<usize>>>;
pub type TakeIdxIterNull<'a, INull> = TakeIdx<'a, Dummy<usize>, INull>;

impl<'a> From<&'a DFStringArray> for TakeIdx<'a, Dummy<usize>, Dummy<Option<usize>>> {
    fn from(ca: &'a DFStringArray) -> Self {
        TakeIdx::Array(ca.downcast_iter().next().unwrap())
    }
}

impl<'a, I> From<I> for TakeIdx<'a, I, Dummy<Option<usize>>>
where I: Iterator<Item = usize>
{
    fn from(iter: I) -> Self {
        TakeIdx::Iter(iter)
    }
}

impl<'a, INulls> From<DataArrayWrap<INulls>> for TakeIdx<'a, Dummy<usize>, INulls>
where INulls: Iterator<Item = Option<usize>>
{
    fn from(iter: DataArrayWrap<INulls>) -> Self {
        TakeIdx::IterNulls(iter.0)
    }
}

/// Fast access by index.
pub trait ArrayTake {
    /// Take values from DataArrayBase by index.
    ///
    /// # Safety
    ///
    /// Doesn't do any bound checking.
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Self
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>;

    /// Take values from DataArrayBase by index.
    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Self
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>;
}

macro_rules! take_random_get {
    ($self:ident, $index:ident) => {{
        match $self {
            Some(arr) => {
                if arr.is_null($index) {
                    None
                } else {
                    // SAFETY:
                    // bounds checked above
                    unsafe { Some(arr.value_unchecked($index)) }
                }
            }
            None => None,
        }
    }};
}

/// Create a type that implements a faster `TakeRandom`.
pub trait IntoTakeRandom<'a> {
    type Item;
    type TakeRandom;
    /// Create a type that implements `TakeRandom`.
    fn take_rand(&self) -> Self::TakeRandom;
}

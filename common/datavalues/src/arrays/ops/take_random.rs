
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeListArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::array::UInt32Array;

use crate::arrays::DataArray;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::series::SeriesWrap;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFUtf8Array;

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

impl<'a, I> From<I> for TakeIdx<'a, I, Dummy<Option<usize>>>
where I: Iterator<Item = usize>
{
    fn from(iter: I) -> Self {
        TakeIdx::Iter(iter)
    }
}

impl<'a, INulls> From<SeriesWrap<INulls>> for TakeIdx<'a, Dummy<usize>, INulls>
where INulls: Iterator<Item = Option<usize>>
{
    fn from(iter: SeriesWrap<INulls>) -> Self {
        TakeIdx::IterNulls(iter.0)
    }
}

macro_rules! take_random_get {
    ($self:ident, $index:ident) => {{
        if $self.arr.is_null($index) {
            None
        } else {
            // Safety:
            // bound checked above
            unsafe { Some($self.arr.value_unchecked($index)) }
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

pub enum TakeRandBranch<N, S> {
    SingleNoNull(N),
    Single(S),
}

impl<N, S, I> TakeRandom for TakeRandBranch<N, S>
where
    N: TakeRandom<Item = I>,
    S: TakeRandom<Item = I>,
{
    type Item = I;

    fn get(&self, index: usize) -> Option<Self::Item> {
        match self {
            Self::SingleNoNull(s) => s.get(index),
            Self::Single(s) => s.get(index),
        }
    }

    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        match self {
            Self::SingleNoNull(s) => s.get_unchecked(index),
            Self::Single(s) => s.get_unchecked(index),
        }
    }
}

impl<'a, T> IntoTakeRandom<'a> for &'a DataArray<T>
where T: DFNumericType
{
    type Item = T::Native;
    type TakeRandom =
        TakeRandBranch<NumTakeRandomCont<'a, T::Native>, NumTakeRandomSingleArray<'a, T>>;

    #[inline]
    fn take_rand(&self) -> Self::TakeRandom {
        if self.null_count() == 0 {
            let t = NumTakeRandomCont {
                slice: self.downcast_ref().values(),
            };
            TakeRandBranch::SingleNoNull(t)
        } else {
            let t = NumTakeRandomSingleArray {
                arr: self.downcast_ref(),
            };
            TakeRandBranch::Single(t)
        }
    }
}

pub struct Utf8TakeRandom<'a> {
    arr: &'a StringArray,
}

impl<'a> TakeRandom for Utf8TakeRandom<'a> {
    type Item = &'a str;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFUtf8Array {
    type Item = &'a str;
    type TakeRandom = TakeRandBranch<Utf8TakeRandom<'a>, Utf8TakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let arr = self.downcast_ref();
        let t = Utf8TakeRandom { arr };
        TakeRandBranch::Single(t)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFBooleanArray {
    type Item = bool;
    type TakeRandom = TakeRandBranch<BoolTakeRandom<'a>, BoolTakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let arr = self.downcast_ref();
        let t = BoolTakeRandom { arr };
        TakeRandBranch::Single(t)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFListArray {
    type Item = Series;
    type TakeRandom = TakeRandBranch<ListTakeRandom<'a>, ListTakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let t = ListTakeRandom {
            arr: self.downcast_ref(),
        };
        TakeRandBranch::Single(t)
    }
}

pub struct NumTakeRandomCont<'a, T> {
    slice: &'a [T],
}

impl<'a, T> TakeRandom for NumTakeRandomCont<'a, T>
where T: Copy
{
    type Item = T;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        self.slice.get(index).copied()
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        *self.slice.get_unchecked(index)
    }
}

pub struct NumTakeRandomSingleArray<'a, T>
where T: DFNumericType
{
    arr: &'a PrimitiveArray<T>,
}

impl<'a, T> TakeRandom for NumTakeRandomSingleArray<'a, T>
where T: DFNumericType
{
    type Item = T::Native;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

pub struct BoolTakeRandom<'a> {
    arr: &'a BooleanArray,
}

impl<'a> TakeRandom for BoolTakeRandom<'a> {
    type Item = bool;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

pub struct ListTakeRandom<'a> {
    arr: &'a LargeListArray,
}

impl<'a> TakeRandom for ListTakeRandom<'a> {
    type Item = Series;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        let v = take_random_get!(self, index);
        v.map(|v| {
            let s = v.into_series();
            s
        })
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let v = self.arr.value_unchecked(index);
        let s = v.into_series();
        s
    }
}

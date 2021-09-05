use std::fmt;
use std::mem;
use std::slice;

use crate::types::Marshal;
use crate::types::StatBuffer;
use crate::types::Unmarshal;

#[derive(Clone)]
pub struct List<T>
where T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static
{
    data: Vec<T>,
}

impl<T> List<T>
where T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static
{
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn at(&self, index: usize) -> T {
        self.data[index]
    }

    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    #[cfg(test)]
    pub fn new() -> List<T> {
        List { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> List<T> {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn resize(&mut self, new_len: usize, value: T) {
        self.data.resize(new_len, value);
    }

    pub(super) unsafe fn set_len(&mut self, new_len: usize) {
        self.data.set_len(new_len);
    }

    pub(super) unsafe fn as_ptr(&self) -> *const T {
        self.data.as_ptr()
    }
}

impl<T> fmt::Debug for List<T>
where T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static + fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl<T> AsRef<[u8]> for List<T>
where T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static
{
    fn as_ref(&self) -> &[u8] {
        let ptr = self.data.as_ptr() as *const u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts(ptr, size) }
    }
}

impl<T> AsMut<[u8]> for List<T>
where T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static
{
    fn as_mut(&mut self) -> &mut [u8] {
        let ptr = self.data.as_mut_ptr() as *mut u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts_mut(ptr, size) }
    }
}

#[cfg(test)]
mod test {
    use std::f64::EPSILON;

    use rand::random;

    use super::*;

    #[test]
    fn test_push_and_len() {
        let mut list = List::with_capacity(100_500);

        for i in 0..100_500 {
            assert_eq!(list.len(), i as usize);
            list.push(i);
        }
    }

    #[test]
    fn test_push_and_get() {
        let mut list = List::<f64>::new();
        let mut vs = vec![0.0_f64; 100];

        for (count, _) in (0..100).enumerate() {
            assert_eq!(list.len(), count);

            for (i, v) in vs.iter().take(count).enumerate() {
                assert!((list.at(i) - *v).abs() < EPSILON);
            }

            let k = random();
            list.push(k);
            vs[count] = k;
        }
    }
}

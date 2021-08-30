use std::borrow::Borrow;

use super::Meter;

pub struct FileSize;

/// Given a tuple of (path, filesize), use the filesize for measurement.
impl<K> Meter<K, u64> for FileSize {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, v: &u64) -> usize
    where K: Borrow<Q> {
        *v as usize
    }
}

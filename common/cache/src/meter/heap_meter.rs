use std::borrow::Borrow;

use heapsize_::HeapSizeOf;

use super::Meter;

/// Size limit based on the heap size of each cache item.
///
/// Requires cache entries that implement [`HeapSizeOf`][1].
///
/// [1]: https://doc.servo.org/heapsize/trait.HeapSizeOf.html
pub struct HeapSize;

impl<K, V: HeapSizeOf> Meter<K, V> for HeapSize {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, item: &V) -> usize
    where K: Borrow<Q> {
        item.heap_size_of_children() + ::std::mem::size_of::<V>()
    }
}

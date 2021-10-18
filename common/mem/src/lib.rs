mod allocators;
mod malloc_size;
mod sizeof;

pub use allocators::MallocSizeOfExt;
pub use malloc_size::MallocShallowSizeOf;
pub use malloc_size::MallocSizeOf;
pub use malloc_size::MallocSizeOfOps;

/// Heap size of structure.
///
/// Structure can be anything that implements MallocSizeOf.
pub fn malloc_size<T: MallocSizeOf + ?Sized>(t: &T) -> usize {
    MallocSizeOf::size_of(t, &mut allocators::new_malloc_size_ops())
}

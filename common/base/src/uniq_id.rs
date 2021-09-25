use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub fn uniq_usize() -> usize {
    static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);

    GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst)
}

mod data;
mod stream;

#[test]
fn mmap_slice() {
    let slice = &[1, 2, 3];
    let array = unsafe { arrow2::ffi::mmap::slice(slice) };
    assert_eq!(array.values().as_ref(), &[1, 2, 3]);
    // note: when `slice` is dropped, array must be dropped as-well since by construction of `slice` they share their lifetimes.
}

#[test]
fn mmap_bitmap() {
    let slice = &[123u8, 255];
    let array = unsafe { arrow2::ffi::mmap::bitmap(slice, 2, 14) }.unwrap();
    assert_eq!(array.values_iter().collect::<Vec<_>>(), &[
        false, true, true, true, true, false, true, true, true, true, true, true, true, true
    ]);
    // note: when `slice` is dropped, array must be dropped as-well since by construction of `slice` they share their lifetimes.
}

use std::cell::UnsafeCell;
use std::ops::Deref;

pub struct UnSafeCellWrap<T: Sized> {
    inner: UnsafeCell<T>,
}

unsafe impl<T: Sized> Send for UnSafeCellWrap<T> {}

unsafe impl<T: Sized> Sync for UnSafeCellWrap<T> {}

impl<T: Sized> UnSafeCellWrap<T> {
    pub fn create(inner: T) -> UnSafeCellWrap<T> {
        UnSafeCellWrap {
            inner: UnsafeCell::new(inner)
        }
    }

    pub fn set_value(&self, value: T) {
        unsafe {
            let inner_value = &mut *self.inner.get();
            *inner_value = value;
        }
    }
}

impl<T: Sized> Deref for UnSafeCellWrap<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}

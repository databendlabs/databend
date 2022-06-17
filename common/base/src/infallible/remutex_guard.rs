use std::cell::UnsafeCell;
use std::ops::Deref;
use std::ops::DerefMut;

use parking_lot::ReentrantMutexGuard as ParkingReentrantMutexGuard;

pub struct ReentrantMutexGuard<'a, T> {
    inner: ParkingReentrantMutexGuard<'a, UnsafeCell<T>>,
}

#[allow(suspicious_auto_trait_impls)]
unsafe impl<'a, T> Send for ReentrantMutexGuard<'a, UnsafeCell<T>> where ParkingReentrantMutexGuard<'a, T>: Send
{}

#[allow(suspicious_auto_trait_impls)]
unsafe impl<'a, T> Sync for ReentrantMutexGuard<'a, UnsafeCell<T>> where ParkingReentrantMutexGuard<'a, T>: Sync
{}

impl<'a, T> ReentrantMutexGuard<'a, T> {
    pub fn create(
        inner: ParkingReentrantMutexGuard<'a, UnsafeCell<T>>,
    ) -> ReentrantMutexGuard<'a, T> {
        ReentrantMutexGuard { inner }
    }
}

impl<'a, T> Deref for ReentrantMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.deref().get() }
    }
}

impl<'a, T> DerefMut for ReentrantMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.deref().get() }
    }
}

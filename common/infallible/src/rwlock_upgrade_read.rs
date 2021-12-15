use std::ops::Deref;
use parking_lot::RwLockUpgradableReadGuard as ParkingUpgradableReadGuard;
use parking_lot::RwLockWriteGuard;

pub struct RwLockUpgradableReadGuard<'a, T: ?Sized>(ParkingUpgradableReadGuard<'a, T>);

impl<'a, T: ?Sized + 'a> RwLockUpgradableReadGuard<'a, T> {
    pub fn create(inner: ParkingUpgradableReadGuard<'a, T>) -> Self {
        Self(inner)
    }

    pub fn upgrade(self) -> RwLockWriteGuard<'a, T> {
        ParkingUpgradableReadGuard::<'a, T>::upgrade(self.0)
    }
}

impl<'a, T: ?Sized + 'a> Deref for RwLockUpgradableReadGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}



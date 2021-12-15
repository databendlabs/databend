use std::ops::Deref;
use parking_lot::RwLockUpgradableReadGuard as ParkingUpgradableReadGuard;
use parking_lot::RwLockWriteGuard;

pub struct RwLockUpgradableReadGuard<'a, T>(ParkingUpgradableReadGuard<'a, T>);

impl<'a, T> RwLockUpgradableReadGuard<'a, T> {
    pub fn upgrade(self) -> RwLockWriteGuard<'a, T> {
        ParkingUpgradableReadGuard::<'a, T>::upgrade(self.0)
    }
}

impl<'a, T> Deref for RwLockUpgradableReadGuard<'a, T> where T: ?Sized + 'a {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}



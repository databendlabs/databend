use parking_lot::Condvar as ParkingCondvar;
use parking_lot::MutexGuard;

pub struct Condvar(ParkingCondvar);

impl Condvar {
    pub fn create() -> Condvar {
        Condvar(ParkingCondvar::new())
    }

    #[inline]
    pub fn notify_one(&self) -> bool {
        self.0.notify_one()
    }

    #[inline]
    pub fn wait<T: ?Sized>(&self, mutex_guard: &mut MutexGuard<'_, T>) {
        self.0.wait(mutex_guard)
    }
}

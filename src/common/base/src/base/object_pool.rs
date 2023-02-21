// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A thread-safe object pool with automatic return and attach/detach semantics
// Forked from: https://github.com/CJP10/object-pool/ with capacity limit

use std::collections::VecDeque;
use std::iter::FromIterator;
use std::mem::forget;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;

use parking_lot::Mutex;

pub type Stack<T> = VecDeque<T>;

pub struct Pool<T> {
    objects: Mutex<Stack<T>>,
    cap: usize,
}

impl<T> Pool<T> {
    #[inline]
    pub fn new(cap: usize) -> Pool<T> {
        Pool {
            objects: Mutex::new(Stack::new()),
            cap,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.objects.lock().is_empty()
    }

    #[inline]
    pub fn try_pull(&self) -> Option<Reusable<T>> {
        self.objects
            .lock()
            .pop_back()
            .map(|data| Reusable::new(self, data))
    }

    #[inline]
    pub fn pull<F: Fn() -> T>(&self, fallback: F) -> Reusable<T> {
        self.try_pull()
            .unwrap_or_else(|| Reusable::new(self, fallback()))
    }

    #[inline]
    pub fn attach(&self, t: T) {
        let mut objects = self.objects.lock();
        if objects.len() < self.cap {
            objects.push_back(t);
        }
    }
}

impl<T> FromIterator<T> for Pool<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let objects = iter.into_iter().collect::<VecDeque<_>>();
        let cap = objects.len();
        Self {
            objects: Mutex::new(objects),
            cap,
        }
    }
}

pub struct Reusable<'a, T> {
    pool: &'a Pool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Reusable<'a, T> {
    #[inline]
    pub fn new(pool: &'a Pool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    #[inline]
    pub fn detach(mut self) -> (&'a Pool<T>, T) {
        let ret = unsafe { (self.pool, self.take()) };
        forget(self);
        ret
    }

    unsafe fn take(&mut self) -> T {
        ManuallyDrop::take(&mut self.data)
    }
}

impl<'a, T> Deref for Reusable<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> DerefMut for Reusable<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T> Drop for Reusable<'a, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.pool.attach(self.take()) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::mem::drop;

    use crate::base::Pool;
    use crate::base::Reusable;

    #[test]
    fn detach() {
        let pool = Pool::new(1);
        pool.attach(vec![]);
        let (pool, mut object) = pool.try_pull().unwrap().detach();
        object.push(1);
        Reusable::new(pool, object);
        assert_eq!(pool.try_pull().unwrap()[0], 1);
    }

    #[test]
    fn detach_then_attach() {
        let pool = Pool::new(1);
        pool.attach(vec![]);
        let (pool, mut object) = pool.try_pull().unwrap().detach();
        object.push(1);
        pool.attach(object);
        assert_eq!(pool.try_pull().unwrap()[0], 1);
    }

    #[test]
    fn pull() {
        let pool = Pool::<Vec<u8>>::new(1);
        pool.attach(vec![]);

        let object1 = pool.try_pull();
        let object2 = pool.try_pull();
        let object3 = pool.pull(Vec::new);

        assert!(object1.is_some());
        assert!(object2.is_none());
        drop(object1);
        drop(object2);
        drop(object3);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn e2e() {
        let pool = Pool::new(10);
        for _i in 0..10 {
            pool.attach(vec![]);
        }
        let mut objects = VecDeque::new();

        for i in 0..10 {
            let mut object = pool.try_pull().unwrap();
            object.push(i);

            objects.push_back(object);
        }

        assert!(pool.try_pull().is_none());
        drop(objects);
        assert!(pool.try_pull().is_some());

        for i in 0..10 {
            let mut object = pool.objects.lock().pop_back().unwrap();
            assert_eq!(object.pop(), Some(i));
        }
    }
}

// Copyright 2021 Datafuse Labs.
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

use std::fmt;

use crate::prelude::*;

impl<'a, T: PrimitiveType> PrimitiveColumn<T> {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> std::slice::Iter<'a, T> {
        self.values.iter()
    }
}

pub struct PrimitiveIter<'a, T: PrimitiveType> {
    inner: std::slice::Iter<'a, T>,
}

impl<'a, T: PrimitiveType> Iterator for PrimitiveIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.inner.next().copied()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.inner.count()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        self.inner.nth(n).copied()
    }

    #[inline]
    fn advance_by(&mut self, n: usize) -> Result<(), usize> {
        self.inner.advance_by(n)
    }

    #[inline]
    fn last(self) -> Option<T> {
        self.inner.last().copied()
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile.
    #[inline]
    fn for_each<F>(mut self, mut f: F)
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        for x in self.by_ref() {
            f(x);
        }
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile.
    #[inline]
    fn all<F>(&mut self, mut f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        for x in self.by_ref() {
            if !f(x) {
                return false;
            }
        }
        true
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile.
    #[inline]
    fn any<F>(&mut self, mut f: F) -> bool
    where
        Self: Sized,
        F: FnMut(Self::Item) -> bool,
    {
        for x in self.by_ref() {
            if f(x) {
                return true;
            }
        }
        false
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile.
    #[inline]
    fn find<P>(&mut self, mut predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        for x in self.by_ref() {
            if predicate(&x) {
                return Some(x);
            }
        }
        None
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile.
    #[inline]
    fn find_map<B, F>(&mut self, mut f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        for x in self.by_ref() {
            if let Some(y) = f(x) {
                return Some(y);
            }
        }
        None
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile. Also, the `assume` avoids a bounds check.
    #[inline]
    fn position<P>(&mut self, mut predicate: P) -> Option<usize>
    where
        Self: Sized,
        P: FnMut(Self::Item) -> bool,
    {
        let n = self.inner.len();
        for (i, x) in self.by_ref().enumerate() {
            if predicate(x) {
                // we are guaranteed to be in bounds by the loop invariant:
                // when `i >= n`, `self.next()` returns `None` and the loop breaks.
                debug_assert!(i < n);
                return Some(i);
            }
        }
        None
    }

    // We override the default implementation, which uses `try_fold`,
    // because this simple implementation generates less LLVM IR and is
    // faster to compile. Also, the `assume` avoids a bounds check.
    #[inline]
    fn rposition<P>(&mut self, mut predicate: P) -> Option<usize>
    where
        P: FnMut(Self::Item) -> bool,
        Self: Sized + ExactSizeIterator + DoubleEndedIterator,
    {
        let n = self.inner.len();
        let mut i = n;
        while let Some(x) = self.next_back() {
            i -= 1;
            if predicate(x) {
                debug_assert!(i < n);
                return Some(i);
            }
        }
        None
    }
}

impl<T: fmt::Debug + PrimitiveType> fmt::Debug for PrimitiveIter<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Iter").field(&self.inner.as_slice()).finish()
    }
}

unsafe impl<T: Sync + PrimitiveType> Sync for PrimitiveIter<'_, T> {}
unsafe impl<T: Sync + PrimitiveType> Send for PrimitiveIter<'_, T> {}

impl<'a, T: PrimitiveType> PrimitiveIter<'a, T> {
    #[inline]
    pub fn new(column: &'a PrimitiveColumn<T>) -> Self {
        PrimitiveIter {
            inner: column.iter(),
        }
    }
    #[must_use]
    pub fn as_slice(&self) -> &'a [T] {
        self.inner.as_slice()
    }
}

impl<T: PrimitiveType> Clone for PrimitiveIter<'_, T> {
    fn clone(&self) -> Self {
        PrimitiveIter {
            inner: self.inner.clone(),
        }
    }
}

impl<T: PrimitiveType> AsRef<[T]> for PrimitiveIter<'_, T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

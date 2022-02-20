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

use std::iter::TrustedLen;

use crate::prelude::*;

impl<'a, T> Iterator for PrimitiveViewer<'a, T>
where
    T: Scalar<Viewer<'a> = Self> + PrimitiveType,
    T: ScalarRef<'a, ScalarType = T>,
    T: Scalar<RefType<'a> = T>,
{
    type Item = T::RefType<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.size {
            return None;
        }

        let old = self.pos;
        self.pos += 1;

        Some(unsafe { *self.values.as_ptr().add(old & self.non_const_mask) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size - self.pos, Some(self.size - self.pos))
    }
}

impl Iterator for BooleanViewer {
    type Item = bool;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.size {
            return None;
        }
        let old = self.pos;
        self.pos += 1;
        Some(unsafe { self.values.get_bit_unchecked(old & self.non_const_mask) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size - self.pos, Some(self.size - self.pos))
    }
}

impl<'a> Iterator for StringViewer<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.size {
            return None;
        }

        let old = self.pos;
        self.pos += 1;

        unsafe { Some(self.col.value_unchecked(old & self.non_const_mask)) }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size - self.pos, Some(self.size - self.pos))
    }
}

/// Some trait impls to improve the performance of iteration

impl<'a, T> ExactSizeIterator for PrimitiveViewer<'a, T>
where
    T: Scalar<Viewer<'a> = Self> + PrimitiveType,
    T: ScalarRef<'a, ScalarType = T>,
    T: Scalar<RefType<'a> = T>,
{
    fn len(&self) -> usize {
        self.size - self.pos
    }
}

unsafe impl<'a, T> TrustedLen for PrimitiveViewer<'a, T>
where
    T: Scalar<Viewer<'a> = Self> + PrimitiveType,
    T: ScalarRef<'a, ScalarType = T>,
    T: Scalar<RefType<'a> = T>,
{
}

impl ExactSizeIterator for BooleanViewer {
    fn len(&self) -> usize {
        self.size - self.pos
    }
}

unsafe impl TrustedLen for BooleanViewer {}

impl<'a> ExactSizeIterator for StringViewer<'a> {
    fn len(&self) -> usize {
        self.size - self.pos
    }
}

unsafe impl<'a> TrustedLen for StringViewer<'a> {}

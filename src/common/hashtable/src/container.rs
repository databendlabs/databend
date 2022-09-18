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

use std::alloc::Allocator;
use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::null_mut;
use std::ptr::NonNull;

pub unsafe trait Container
where Self: Deref<Target = [Self::T]> + DerefMut
{
    type T;

    type A: Allocator;

    fn len(&self) -> usize;

    unsafe fn new_zeroed(len: usize, allocator: Self::A) -> Self;

    unsafe fn grow_zeroed(&mut self, new_len: usize);
}

#[derive(Debug)]
pub struct HeapContainer<T, A: Allocator>(Box<[T], A>);

impl<T, A: Allocator> Deref for HeapContainer<T, A> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, A: Allocator> DerefMut for HeapContainer<T, A> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl<T, A: Allocator> Container for HeapContainer<T, A> {
    type T = T;

    type A = A;

    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    unsafe fn new_zeroed(len: usize, allocator: Self::A) -> Self {
        Self(Box::new_zeroed_slice_in(len, allocator).assume_init())
    }

    unsafe fn grow_zeroed(&mut self, new_len: usize) {
        debug_assert!(self.len() <= new_len);
        let old_layout = Layout::array::<T>(self.len()).unwrap();
        let new_layout = Layout::array::<T>(new_len).unwrap();
        let old_box = std::ptr::read(&self.0);
        let (old_raw, allocator) = Box::into_raw_with_allocator(old_box);
        let old_ptr = NonNull::new(old_raw).unwrap().cast();
        let new_ptr = allocator
            .grow_zeroed(old_ptr, old_layout, new_layout)
            .unwrap();
        let new_raw = std::ptr::slice_from_raw_parts_mut(new_ptr.cast().as_ptr(), new_len);
        let new_box = Box::from_raw_in(new_raw, allocator);
        std::ptr::write(self, Self(new_box));
    }
}

pub struct StackContainer<T, const N: usize, A: Allocator> {
    allocator: A,
    ptr: *mut T,
    len: usize,
    array: [MaybeUninit<T>; N],
}

impl<T, const N: usize, A: Allocator> Deref for StackContainer<T, N, A> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe {
            std::slice::from_raw_parts(
                if self.ptr.is_null() {
                    self.array.as_ptr() as *const _
                } else {
                    self.ptr
                },
                self.len,
            )
        }
    }
}

impl<T, const N: usize, A: Allocator> DerefMut for StackContainer<T, N, A> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(
                if self.ptr.is_null() {
                    self.array.as_mut_ptr() as *mut _
                } else {
                    self.ptr
                },
                self.len,
            )
        }
    }
}

unsafe impl<T, const N: usize, A: Allocator + Clone> Container for StackContainer<T, N, A> {
    type T = T;

    type A = A;

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    unsafe fn new_zeroed(len: usize, allocator: Self::A) -> Self {
        if len <= N {
            Self {
                allocator,
                ptr: null_mut(),
                len,
                array: std::array::from_fn(|_| MaybeUninit::zeroed()),
            }
        } else {
            let ptr = allocator
                .allocate_zeroed(Layout::array::<T>(len).unwrap())
                .unwrap()
                .cast()
                .as_ptr();
            Self {
                allocator,
                ptr,
                len,
                array: std::array::from_fn(|_| MaybeUninit::uninit()),
            }
        }
    }

    unsafe fn grow_zeroed(&mut self, new_len: usize) {
        debug_assert!(self.len <= new_len);
        if new_len <= N {
            self.len = new_len;
        } else if self.ptr.is_null() {
            self.ptr = self
                .allocator
                .allocate_zeroed(Layout::array::<T>(new_len).unwrap())
                .unwrap()
                .cast()
                .as_ptr();
            std::ptr::copy_nonoverlapping(self.array.as_ptr() as *mut _, self.ptr, self.len);
            self.len = new_len;
        } else {
            let old_layout = Layout::array::<T>(self.len).unwrap();
            let new_layout = Layout::array::<T>(new_len).unwrap();
            self.ptr = self
                .allocator
                .grow_zeroed(
                    NonNull::new_unchecked(self.ptr).cast(),
                    old_layout,
                    new_layout,
                )
                .unwrap()
                .cast::<T>()
                .as_ptr();
            self.len = new_len;
        }
    }
}

impl<T, const N: usize, A: Allocator> Drop for StackContainer<T, N, A> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                self.allocator.deallocate(
                    NonNull::new(self.ptr).unwrap().cast(),
                    Layout::array::<T>(self.len).unwrap(),
                );
            }
        }
    }
}

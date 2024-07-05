// Copyright 2021 Datafuse Labs
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

use core::fmt;
use std::cmp::Reverse;
use std::collections::binary_heap;
use std::collections::BinaryHeap;
use std::ops::Deref;
use std::ops::DerefMut;

use super::loser_tree;
use super::utils::find_bigger_child_of_root;
use super::Cursor;
use super::Rows;

pub trait SortAlgorithm {
    type Rows: Rows;
    type PeekMut<'b>: Deref<Target = Reverse<Cursor<Self::Rows>>> + DerefMut
    where Self: 'b;
    fn with_capacity(capacity: usize) -> Self;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize;

    fn rebuild(&mut self);

    fn push(&mut self, index: usize, item: Reverse<Cursor<Self::Rows>>);

    fn pop(&mut self);

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>);

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>>;

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>>;

    fn peek_mut(&mut self) -> Self::PeekMut<'_>;

    fn pop_mut(this: Self::PeekMut<'_>);
}

pub type HeapSort<R> = BinaryHeap<Reverse<Cursor<R>>>;

impl<R: Rows> SortAlgorithm for BinaryHeap<Reverse<Cursor<R>>> {
    type Rows = R;
    type PeekMut<'a> = binary_heap::PeekMut<'a, Reverse<Cursor<R>>> where R:'a;
    fn with_capacity(capacity: usize) -> Self {
        BinaryHeap::with_capacity(capacity)
    }

    fn len(&self) -> usize {
        BinaryHeap::len(self)
    }

    fn rebuild(&mut self) {}

    fn push(&mut self, _index: usize, item: Reverse<Cursor<Self::Rows>>) {
        BinaryHeap::push(self, item)
    }

    fn pop(&mut self) {
        BinaryHeap::pop(self);
    }

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>) {
        // `peek_mut` will return a `PeekMut` object which allows us to modify the top element of the heap.
        // The heap will adjust itself automatically when the `PeekMut` object is dropped (RAII).
        *BinaryHeap::peek_mut(self).unwrap() = item
    }

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>> {
        BinaryHeap::peek(self)
    }

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>> {
        find_bigger_child_of_root(self)
    }

    fn peek_mut(&mut self) -> Self::PeekMut<'_> {
        BinaryHeap::peek_mut(self).unwrap()
    }

    fn pop_mut(this: Self::PeekMut<'_>) {
        binary_heap::PeekMut::pop(this);
    }
}

pub struct LoserTreeSort<R: Rows> {
    tree: loser_tree::LoserTree<Option<Reverse<Cursor<R>>>>,
    length: usize,
}

impl<R: Rows> fmt::Debug for LoserTreeSort<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self
            .tree
            .data()
            .iter()
            .map(|x| x.as_ref().map(|Reverse(cursor)| cursor.row_index))
            .collect::<Vec<_>>();

        f.debug_struct("LoserTreeSort")
            .field("tree", &self.tree.tree())
            .field("data", &data)
            .field("length", &self.length)
            .finish()
    }
}

impl<R: Rows> SortAlgorithm for LoserTreeSort<R> {
    type Rows = R;
    type PeekMut<'a> = LoserTreePeekMut<'a,Self::Rows>  where Self: 'a;
    fn with_capacity(capacity: usize) -> Self {
        let data = vec![None; capacity];
        LoserTreeSort {
            tree: loser_tree::LoserTree::from(data),
            length: 0,
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn rebuild(&mut self) {
        self.tree.rebuild()
    }

    fn push(&mut self, index: usize, item: Reverse<Cursor<Self::Rows>>) {
        self.tree.update(index, Some(item));
        self.length += 1
    }

    fn pop(&mut self) {
        debug_assert!(self.length >= 1);
        self.tree.peek_mut().take();
        self.tree.adjust_top();
        self.length -= 1;
    }

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>) {
        self.tree.update(self.tree.winner(), Some(item))
    }

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>> {
        self.tree.peek().as_ref()
    }

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>> {
        self.tree.peek_top2().as_ref().unwrap()
    }

    fn peek_mut(&mut self) -> Self::PeekMut<'_> {
        LoserTreePeekMut(self)
    }

    fn pop_mut(this: Self::PeekMut<'_>) {
        debug_assert!(this.0.length >= 1);
        this.0.tree.peek_mut().take();
        this.0.length -= 1;
        // The tree will adjust itself automatically when the `PeekMut` object is dropped (RAII).
    }
}

pub struct LoserTreePeekMut<'a, R: Rows>(&'a mut LoserTreeSort<R>);

impl<R: Rows> Deref for LoserTreePeekMut<'_, R> {
    type Target = Reverse<Cursor<R>>;

    fn deref(&self) -> &Self::Target {
        self.0.tree.peek().as_ref().unwrap()
    }
}

impl<R: Rows> DerefMut for LoserTreePeekMut<'_, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.tree.peek_mut().as_mut().unwrap()
    }
}

impl<R: Rows> Drop for LoserTreePeekMut<'_, R> {
    fn drop(&mut self) {
        self.0.tree.adjust_top();
    }
}

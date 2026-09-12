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
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::binary_heap;
use std::ops::Deref;
use std::ops::DerefMut;

use super::Cursor as RawCursor;
use super::CursorOrder;
use super::Rows;
use super::loser_tree;
use super::utils::find_bigger_child_of_root;

pub type Cursor<R> = RawCursor<'static, R, ItemCursorOrder>;

/// Backend for the k-way merge cursor set.
///
/// Each input contributes one cursor positioned at its current row. The merge
/// loop performs four steps:
///
/// 1. Peek the cursor with the smallest current row.
/// 2. Find a consecutive range from that input which can be emitted without
///    another cursor becoming the winner.
/// 3. Emit the range and advance its cursor.
/// 4. Remove an exhausted cursor, or repair the cursor set after its key changes.
///
/// The loop repeats once per drained range, not once per output row. Its cost can
/// be summarized as:
///
/// `row work + R * per-range work`
///
/// `HeapSort` and `LoserTreeTop2Sort` expose the second cursor, allowing the merger to
/// drain up to that key. `LoserTreeSort` keeps the winner in the tree and
/// only coalesces equal keys because that layout does not expose the second cursor.
pub trait SortAlgorithm: Send {
    const SHOULD_PEEK_TOP2: bool = true;
    type Rows: Rows;
    type PeekMut<'b>: Deref<Target = Cursor<Self::Rows>> + DerefMut
    where Self: 'b;
    fn with_capacity(capacity: usize) -> Self;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize;

    fn rebuild(&mut self);

    fn push(&mut self, index: usize, item: Cursor<Self::Rows>);

    fn pop(&mut self);

    fn update_top(&mut self, item: Cursor<Self::Rows>);

    fn peek(&self) -> Option<&Cursor<Self::Rows>>;

    fn peek_top2(&self) -> &Cursor<Self::Rows>;

    fn peek_mut(&mut self) -> Self::PeekMut<'_>;

    fn pop_mut(this: Self::PeekMut<'_>);
}

pub type HeapSort<R> = BinaryHeap<Cursor<R>>;

impl<R: Rows> SortAlgorithm for BinaryHeap<Cursor<R>> {
    type Rows = R;
    type PeekMut<'a>
        = binary_heap::PeekMut<'a, Cursor<R>>
    where R: 'a;
    fn with_capacity(capacity: usize) -> Self {
        BinaryHeap::with_capacity(capacity)
    }

    fn len(&self) -> usize {
        BinaryHeap::len(self)
    }

    fn rebuild(&mut self) {}

    fn push(&mut self, _index: usize, item: Cursor<Self::Rows>) {
        BinaryHeap::push(self, item)
    }

    fn pop(&mut self) {
        BinaryHeap::pop(self);
    }

    fn update_top(&mut self, item: Cursor<Self::Rows>) {
        // `peek_mut` will return a `PeekMut` object which allows us to modify the top element of the heap.
        // The heap will adjust itself automatically when the `PeekMut` object is dropped (RAII).
        *BinaryHeap::peek_mut(self).unwrap() = item
    }

    fn peek(&self) -> Option<&Cursor<Self::Rows>> {
        BinaryHeap::peek(self)
    }

    fn peek_top2(&self) -> &Cursor<Self::Rows> {
        find_bigger_child_of_root(self)
    }

    fn peek_mut(&mut self) -> Self::PeekMut<'_> {
        BinaryHeap::peek_mut(self).unwrap()
    }

    fn pop_mut(this: Self::PeekMut<'_>) {
        binary_heap::PeekMut::pop(this);
    }
}

pub struct LoserTreeTop2Sort<R: Rows> {
    top: Option<Cursor<R>>,
    tree: loser_tree::LoserTree<Cursor<R>>,
    exhausted_input: Option<usize>,
    staged: Vec<(usize, Cursor<R>)>,
    length: usize,
}

impl<R: Rows> fmt::Debug for LoserTreeTop2Sort<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tree = self
            .tree
            .nodes()
            .iter()
            .map(|node| {
                node.as_ref()
                    .map(|(slot, cursor)| (*slot, cursor.row_index))
            })
            .collect::<Vec<_>>();
        let staged = self
            .staged
            .iter()
            .map(|(index, cursor)| (*index, cursor.row_index))
            .collect::<Vec<_>>();

        f.debug_struct("LoserTreeTop2Sort")
            .field("top", &self.top.as_ref().map(|cursor| cursor.row_index))
            .field("tree", &tree)
            .field("exhausted_input", &self.exhausted_input)
            .field("staged", &staged)
            .field("length", &self.length)
            .finish()
    }
}

impl<R: Rows> LoserTreeTop2Sort<R> {
    #[inline(always)]
    fn reconcile_top(&mut self) {
        if self.exhausted_input.is_some() || !self.staged.is_empty() {
            return;
        }
        if let Some(top) = self.top
            && self.tree.peek().is_some_and(|second| top < *second)
        {
            self.top = Some(self.tree.promote_with(Some(top)));
        }
    }

    #[inline(always)]
    fn pop_top(&mut self) {
        let top = self.top.take().expect("loser tree sort must not be empty");
        debug_assert!(self.exhausted_input.is_none());
        self.exhausted_input = Some(top.input_index);
        self.length -= 1;
    }

    fn rebuild_all(&mut self) {
        let mut values = Vec::with_capacity(self.length);
        values.extend(self.tree.values());
        values.extend(self.staged.drain(..).map(|(_, cursor)| cursor));

        if self.top.is_none() {
            self.top = values.pop();
        }
        debug_assert_eq!(values.len() + usize::from(self.top.is_some()), self.length);
        self.tree.rebuild_from(values);
        self.reconcile_top();
    }

    fn prepare_for_read(&mut self) {
        let Some(exhausted_input) = self.exhausted_input.take() else {
            if !self.staged.is_empty() {
                self.rebuild_all();
            }
            return;
        };

        // Refill normally stages at most one cursor, from the stream whose
        // previous block just exhausted. Replay that replacement on the tree
        // winner's path while promoting the winner to the external top.
        let replacement = match self.staged.as_slice() {
            [] => None,
            [(index, cursor)] if *index == exhausted_input => Some(*cursor),
            _ => {
                self.rebuild_all();
                return;
            }
        };
        self.staged.clear();

        self.top = if self.tree.peek().is_some() {
            Some(self.tree.promote_with(replacement))
        } else {
            replacement
        };
        self.reconcile_top();
    }
}

impl<R: Rows> SortAlgorithm for LoserTreeTop2Sort<R> {
    type Rows = R;
    type PeekMut<'a>
        = LoserTreeTop2PeekMut<'a, Self::Rows>
    where Self: 'a;
    fn with_capacity(capacity: usize) -> Self {
        LoserTreeTop2Sort {
            top: None,
            tree: loser_tree::LoserTree::with_capacity(capacity.saturating_sub(1)),
            exhausted_input: None,
            staged: Vec::with_capacity(capacity),
            length: 0,
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn rebuild(&mut self) {
        self.prepare_for_read();
    }

    fn push(&mut self, index: usize, item: Cursor<Self::Rows>) {
        self.length += 1;
        self.staged.push((index, item));
    }

    fn pop(&mut self) {
        self.pop_top();
        self.prepare_for_read();
    }

    fn update_top(&mut self, item: Cursor<Self::Rows>) {
        self.top = Some(item);
        self.reconcile_top();
    }

    fn peek(&self) -> Option<&Cursor<Self::Rows>> {
        debug_assert!(self.exhausted_input.is_none() && self.staged.is_empty());
        self.top.as_ref()
    }

    fn peek_top2(&self) -> &Cursor<Self::Rows> {
        debug_assert!(self.exhausted_input.is_none() && self.staged.is_empty());
        self.tree
            .peek()
            .expect("top2 requires at least two cursors")
    }

    fn peek_mut(&mut self) -> Self::PeekMut<'_> {
        LoserTreeTop2PeekMut(self)
    }

    fn pop_mut(this: Self::PeekMut<'_>) {
        this.0.pop_top();
    }
}

pub struct LoserTreeTop2PeekMut<'a, R: Rows>(&'a mut LoserTreeTop2Sort<R>);

impl<R: Rows> Deref for LoserTreeTop2PeekMut<'_, R> {
    type Target = Cursor<R>;

    fn deref(&self) -> &Self::Target {
        self.0.top.as_ref().unwrap()
    }
}

impl<R: Rows> DerefMut for LoserTreeTop2PeekMut<'_, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.top.as_mut().unwrap()
    }
}

impl<R: Rows> Drop for LoserTreeTop2PeekMut<'_, R> {
    fn drop(&mut self) {
        self.0.reconcile_top();
    }
}

/// A loser tree which keeps the winner in the tree itself.
///
/// Unlike [`LoserTreeTop2Sort`], this layout cannot expose the second cursor without
/// searching the winner path. The merger therefore only coalesces equal keys for
/// this implementation.
pub struct LoserTreeSort<R: Rows> {
    tree: loser_tree::LoserTree<Cursor<R>>,
    exhausted: Option<(usize, usize)>,
    staged: Vec<(usize, Cursor<R>)>,
    length: usize,
}

impl<R: Rows> fmt::Debug for LoserTreeSort<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tree = self
            .tree
            .nodes()
            .iter()
            .map(|node| {
                node.as_ref()
                    .map(|(slot, cursor)| (*slot, cursor.row_index))
            })
            .collect::<Vec<_>>();
        let staged = self
            .staged
            .iter()
            .map(|(index, cursor)| (*index, cursor.row_index))
            .collect::<Vec<_>>();

        f.debug_struct("LoserTreeSort")
            .field("tree", &tree)
            .field("exhausted", &self.exhausted)
            .field("staged", &staged)
            .field("length", &self.length)
            .finish()
    }
}

impl<R: Rows> LoserTreeSort<R> {
    fn rebuild_all(&mut self) {
        let mut values = Vec::with_capacity(self.length);
        values.extend(self.tree.values());
        values.extend(self.staged.drain(..).map(|(_, cursor)| cursor));
        debug_assert_eq!(values.len(), self.length);
        self.tree.rebuild_from(values);
    }

    #[inline(always)]
    fn pop_top(&mut self) {
        let (slot, top) = self.tree.take_top();
        debug_assert!(self.exhausted.is_none());
        self.exhausted = Some((top.input_index, slot));
        self.length -= 1;
    }

    fn prepare_for_read(&mut self) {
        let Some((exhausted_input, slot)) = self.exhausted.take() else {
            if !self.staged.is_empty() {
                self.rebuild_all();
            }
            return;
        };

        let replacement = match self.staged.as_slice() {
            [] => None,
            [(index, cursor)] if *index == exhausted_input => Some(*cursor),
            _ => {
                self.tree.replace_top(slot, None);
                self.rebuild_all();
                return;
            }
        };
        self.staged.clear();

        self.tree.replace_top(slot, replacement);
    }
}

impl<R: Rows> SortAlgorithm for LoserTreeSort<R> {
    const SHOULD_PEEK_TOP2: bool = false;
    type Rows = R;
    type PeekMut<'a>
        = LoserTreePeekMut<'a, Self::Rows>
    where Self: 'a;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            tree: loser_tree::LoserTree::with_capacity(capacity),
            exhausted: None,
            staged: Vec::with_capacity(capacity),
            length: 0,
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn rebuild(&mut self) {
        self.prepare_for_read();
    }

    fn push(&mut self, index: usize, item: Cursor<Self::Rows>) {
        self.staged.push((index, item));
        self.length += 1;
    }

    fn pop(&mut self) {
        self.pop_top();
        self.prepare_for_read();
    }

    fn update_top(&mut self, item: Cursor<Self::Rows>) {
        *self
            .tree
            .peek_mut()
            .expect("loser tree sort must not be empty") = item;
        self.tree.adjust_top();
    }

    fn peek(&self) -> Option<&Cursor<Self::Rows>> {
        debug_assert!(self.exhausted.is_none() && self.staged.is_empty());
        self.tree.peek()
    }

    fn peek_top2(&self) -> &Cursor<Self::Rows> {
        unreachable!("embedded loser tree does not expose top2")
    }

    fn peek_mut(&mut self) -> Self::PeekMut<'_> {
        LoserTreePeekMut {
            sort: self,
            adjust_on_drop: true,
        }
    }

    fn pop_mut(mut this: Self::PeekMut<'_>) {
        this.sort.pop_top();
        this.adjust_on_drop = false;
    }
}

pub struct LoserTreePeekMut<'a, R: Rows> {
    sort: &'a mut LoserTreeSort<R>,
    adjust_on_drop: bool,
}

impl<R: Rows> Deref for LoserTreePeekMut<'_, R> {
    type Target = Cursor<R>;

    fn deref(&self) -> &Self::Target {
        self.sort.tree.peek().unwrap()
    }
}

impl<R: Rows> DerefMut for LoserTreePeekMut<'_, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sort.tree.peek_mut().unwrap()
    }
}

impl<R: Rows> Drop for LoserTreePeekMut<'_, R> {
    fn drop(&mut self) {
        if self.adjust_on_drop {
            self.sort.tree.adjust_top();
        }
    }
}

#[derive(Clone, Copy)]
pub struct ItemCursorOrder;

impl<R: Rows> CursorOrder<R> for ItemCursorOrder {
    fn eq<'a>(a: &RawCursor<'a, R, Self>, b: &RawCursor<'a, R, Self>) -> bool {
        a.current() == b.current()
    }

    fn cmp<'a>(a: &RawCursor<'a, R, Self>, b: &RawCursor<'a, R, Self>) -> Ordering {
        b.current().cmp(&a.current())
    }
}

// Copyright 2024 Datafuse Labs
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

use core::ops::{Deref, DerefMut};

pub struct LoserTree<T: Ord> {
    tree: Vec<Option<usize>>,
    nulls: Vec<bool>,
    data: Vec<T>,
    length: usize,
}

impl<T: Ord> LoserTree<T> {
    pub fn from(vec: Vec<T>) -> LoserTree<T> {
        let length = vec.len();
        let mut tree = LoserTree {
            tree: vec![None; length],
            nulls: vec![false; length],
            data: vec,
            length: length,
        };
        for i in 0..tree.data.len() {
            tree.adjust(i)
        }
        tree
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn pop(&mut self) -> Option<&T> {
        if self.length > 0 {
            self.length -= 1;
            let win = self.tree[0].unwrap();
            self.nulls[win] = true;
            self.adjust(win);
            return Some(&self.data[win]);
        }
        None
    }

    pub fn peek(&self) -> Option<&T> {
        if self.length > 0 {
            let win = self.tree[0].unwrap();
            return Some(&self.data[win]);
        }
        None
    }

    pub fn peek_top2(&self) -> Option<&T> {
        match self.length {
            0 | 1 => return None,
            _ => (),
        }

        let top = self.tree[0].unwrap();
        let mut top2 = top;
        let mut father_loc = (top2 + self.data.len()) / 2;
        while father_loc > 0 {
            if let Some(father) = self.tree[father_loc] {
                if self.nulls[father] {
                    father_loc /= 2;
                    continue;
                }
                if top2 == top
                    || self.nulls[top2]
                    || self.data[top2] > self.data[father]
                {
                    top2 = father;
                }
                father_loc /= 2;
            }
        }
        Some(&self.data[top2])
    }

    pub fn peek_mut(&mut self) -> Option<PeekMut<T>> {
        if self.is_empty() {
            None
        } else {
            Some(PeekMut { tree: self })
        }
    }

    fn adjust(&mut self, index: usize) {
        let mut winner: usize = index;
        let mut father_loc = (winner + self.data.len()) / 2;
        while father_loc > 0 {
            match self.tree[father_loc] {
                Some(father) => {
                    if self.nulls[father] {
                        father_loc /= 2;
                        continue;
                    }
                    if self.nulls[winner] || self.data[winner] > self.data[father] {
                        self.tree[father_loc] = Some(winner);
                        winner = father;
                    }
                    father_loc /= 2;
                }
                None => {
                    self.tree[father_loc] = Some(winner);
                    break;
                }
            }
        }
        self.tree[0] = Some(winner);
    }
}

pub struct PeekMut<'a, T: 'a + Ord> {
    tree: &'a mut LoserTree<T>,
}

impl<T: Ord> Drop for PeekMut<'_, T> {
    fn drop(&mut self) {
        let win = self.tree.tree[0].unwrap();
        self.tree.adjust(win)
    }
}

impl<T: Ord> Deref for PeekMut<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        debug_assert!(!self.tree.is_empty());

        let win = self.tree.tree[0].unwrap();
        self.tree.data.get(win).unwrap()
    }
}

impl<T: Ord> DerefMut for PeekMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        debug_assert!(!self.tree.is_empty());

        let win = self.tree.tree[0].unwrap();
        self.tree.data.get_mut(win).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_even() {
        let data = vec![4, 6, 5, 9, 8, 2, 3, 7];
        let mut loser_tree = LoserTree::from(data);

        assert_eq!(loser_tree.is_empty(), false);
        for i in 2..=9 {
            assert_eq!(loser_tree.len(), 10 - i);
            assert_eq!(loser_tree.peek(), Some(11-i).as_ref());
            assert_eq!(loser_tree.peek(), Some(11-i).as_ref());
            if i == 9 {
                assert_eq!(loser_tree.peek_top2(), None);
                assert_eq!(loser_tree.peek_top2(), None);
            } else {
                assert_eq!(loser_tree.peek_top2(), Some(10-i).as_ref());
                assert_eq!(loser_tree.peek_top2(), Some(10-i).as_ref());
            }
            assert_eq!(loser_tree.pop(), Some(11-i).as_ref());
        }

        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.len(), 0);
        assert_eq!(loser_tree.peek(), None);
        assert_eq!(loser_tree.peek_top2(), None);
        assert_eq!(loser_tree.is_empty(), true);
    }

    #[test]
    fn basic_odd() {
        let data = vec![4, 6, 5];
        let mut loser_tree = LoserTree::from(data);

        assert_eq!(loser_tree.pop(), Some(6).as_ref());
        assert_eq!(loser_tree.pop(), Some(5).as_ref());
        assert_eq!(loser_tree.pop(), Some(4).as_ref());
        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.pop(), None);
    }

    #[test]
    fn peek_mut() {
        let data = vec![4, 6, 7];
        let mut loser_tree = LoserTree::from(data);

        *loser_tree.peek_mut().unwrap() = 5;
        *loser_tree.peek_mut().unwrap() = 8;

        assert_eq!(loser_tree.pop(), Some(8).as_ref());
        assert_eq!(loser_tree.pop(), Some(5).as_ref());
        assert_eq!(loser_tree.pop(), Some(4).as_ref());
        assert_eq!(loser_tree.pop(), None);
    }
}

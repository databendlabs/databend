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

use core::ops::Deref;
use core::ops::DerefMut;

pub struct LoserTree<T: Ord> {
    tree: Vec<Option<usize>>,
    data: Vec<T>,
}

impl<T: Ord> LoserTree<T> {
    pub fn from(data: Vec<T>) -> Self {
        let length = data.len();
        let mut tree = LoserTree {
            tree: vec![None; length],
            data,
        };
        for i in 0..tree.data.len() {
            tree.adjust(i)
        }
        tree
    }

    pub fn update(&mut self, i: usize, v: T) {
        self.data[i] = v;
        self.adjust(i)
    }

    pub fn winner(&self) -> usize {
        self.tree[0].unwrap()
    }

    pub fn peek(&self) -> &T {
        &self.data[self.winner()]
    }

    pub fn peek_top2(&self) -> &T {
        let top = self.winner();
        let mut top2 = top;
        let mut father_loc = (top2 + self.data.len()) / 2;
        while father_loc > 0 {
            if let Some(father) = self.tree[father_loc] {
                if top2 == top || self.data[top2] < self.data[father] {
                    top2 = father;
                }
                father_loc /= 2;
            }
        }
        &self.data[top2]
    }

    pub fn peek_mut(&mut self) -> PeekMut<T> {
        PeekMut { tree: self }
    }

    fn adjust(&mut self, index: usize) {
        let mut winner: usize = index;
        let mut father_loc = (winner + self.data.len()) / 2;
        while father_loc > 0 {
            match self.tree[father_loc] {
                None => {
                    self.tree[father_loc] = Some(winner);
                    break;
                }
                Some(father) => {
                    if self.data[winner] < self.data[father] {
                        self.tree[father_loc] = Some(winner);
                        winner = father;
                    }
                    father_loc /= 2;
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
        let win = self.tree.tree[0].unwrap();
        self.tree.data.get(win).unwrap()
    }
}

impl<T: Ord> DerefMut for PeekMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        let win = self.tree.tree[0].unwrap();
        self.tree.data.get_mut(win).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let data = vec![
            Some(4),
            Some(6),
            Some(5),
            Some(9),
            Some(8),
            Some(2),
            Some(3),
            Some(7),
        ];
        let mut loser_tree = LoserTree::from(data);

        for i in 2..=9 {
            assert_eq!(*loser_tree.peek(), Some(11 - i));
            assert_eq!(*loser_tree.peek(), Some(11 - i));
            if i == 9 {
                assert_eq!(*loser_tree.peek_top2(), None);
                assert_eq!(*loser_tree.peek_top2(), None);
            } else {
                assert_eq!(*loser_tree.peek_top2(), Some(10 - i));
                assert_eq!(*loser_tree.peek_top2(), Some(10 - i));
            }
            let i = loser_tree.winner();
            loser_tree.update(i, None);
        }
        assert_eq!(*loser_tree.peek(), None);
        assert_eq!(*loser_tree.peek_top2(), None);
    }

    #[test]
    fn peek_mut() {
        let data = vec![4, 6, 7];
        let mut loser_tree = LoserTree::from(data);

        assert_eq!(loser_tree.winner(), 2);
        assert_eq!(*loser_tree.peek_mut(), 7);
        *loser_tree.peek_mut() = 5;

        assert_eq!(loser_tree.winner(), 1);
        assert_eq!(*loser_tree.peek_mut(), 6);
        *loser_tree.peek_mut() = 3;

        assert_eq!(loser_tree.winner(), 2);
        assert_eq!(*loser_tree.peek_mut(), 5);
    }
}

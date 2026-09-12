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

type Node<T> = Option<(usize, T)>;

pub struct LoserTree<T: Ord + Copy> {
    tree: Vec<Node<T>>,
}

impl<T: Ord + Copy> LoserTree<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tree: Vec::with_capacity(capacity),
        }
    }

    #[inline(always)]
    pub fn peek(&self) -> Option<&T> {
        self.tree.first()?.as_ref().map(|(_, value)| value)
    }

    pub fn peek_mut(&mut self) -> Option<&mut T> {
        self.tree.first_mut()?.as_mut().map(|(_, value)| value)
    }

    pub fn rebuild_from(&mut self, values: Vec<T>) {
        self.tree.clear();
        self.tree.resize(values.len(), None);
        for (slot, value) in values.into_iter().enumerate() {
            self.adjust::<true>(slot, value);
        }
    }

    pub fn values(&self) -> impl Iterator<Item = T> + '_ {
        self.tree.iter().flatten().map(|(_, value)| *value)
    }

    pub fn nodes(&self) -> &[Node<T>] {
        &self.tree
    }

    #[inline(always)]
    pub fn adjust_top(&mut self) {
        let (slot, value) = self.tree[0].expect("loser tree must not be empty");
        self.adjust::<false>(slot, value);
    }

    #[inline(always)]
    pub fn take_top(&mut self) -> (usize, T) {
        self.tree[0].take().expect("loser tree must not be empty")
    }

    #[inline(always)]
    pub fn replace_top(&mut self, slot: usize, replacement: Option<T>) {
        debug_assert!(self.tree[0].is_none());
        match replacement {
            Some(replacement) => self.adjust::<false>(slot, replacement),
            None => self.remove(slot),
        }
    }

    /// Removes the winner while optionally replaying its replacement on the same path.
    #[inline(always)]
    pub fn promote_with(&mut self, replacement: Option<T>) -> T {
        let (slot, value) = self.tree[0].expect("loser tree must not be empty");
        match replacement {
            Some(replacement) => self.adjust::<false>(slot, replacement),
            None => self.remove(slot),
        }
        value
    }

    #[inline(always)]
    fn adjust<const BUILD: bool>(&mut self, slot: usize, value: T) {
        let mut winner = (slot, value);
        let mut father_loc = (slot + self.tree.len()) / 2;
        while father_loc > 0 {
            match self.tree[father_loc] {
                None if BUILD => {
                    self.tree[father_loc] = Some(winner);
                    break;
                }
                Some(father) => {
                    if winner.1 < father.1 {
                        self.tree[father_loc] = Some(winner);
                        winner = father;
                    }
                }
                None => {}
            }
            father_loc /= 2;
        }
        self.tree[0] = Some(winner);
    }

    fn remove(&mut self, slot: usize) {
        let mut winner = (slot, None);
        let mut father_loc = (slot + self.tree.len()) / 2;
        while father_loc > 0 {
            if let Some(father) = self.tree[father_loc]
                && winner.1 < Some(father.1)
            {
                self.tree[father_loc] = winner.1.map(|value| (winner.0, value));
                winner = (father.0, Some(father.1));
            }
            father_loc /= 2;
        }
        self.tree[0] = winner.1.map(|value| (winner.0, value));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let data = vec![4, 6, 5, 9, 8, 2, 3, 7];
        let mut tree = LoserTree::with_capacity(data.len());
        tree.rebuild_from(data);

        for expected in (2..=9).rev() {
            assert_eq!(tree.peek(), Some(&expected));
            assert_eq!(tree.promote_with(None), expected);
        }
        assert_eq!(tree.peek(), None);
    }

    #[test]
    fn in_place_update() {
        let mut tree = LoserTree::with_capacity(5);
        tree.rebuild_from(vec![9, 6, 5, 7, 8]);

        assert_eq!(tree.promote_with(Some(4)), 9);
        assert_eq!(tree.peek(), Some(&8));

        assert_eq!(tree.promote_with(Some(3)), 8);
        assert_eq!(tree.peek(), Some(&7));

        assert_eq!(tree.promote_with(Some(2)), 7);
        assert_eq!(tree.peek(), Some(&6));
    }

    #[test]
    fn replace_winner_on_same_path() {
        let mut tree = LoserTree::with_capacity(2);
        tree.rebuild_from(vec![2, 1]);

        assert_eq!(tree.promote_with(Some(3)), 2);
        assert_eq!(tree.promote_with(None), 3);
        assert_eq!(tree.promote_with(None), 1);
        assert_eq!(tree.peek(), None);
    }

    #[test]
    fn replace_taken_top() {
        let mut tree = LoserTree::with_capacity(3);
        tree.rebuild_from(vec![3, 2, 1]);

        let (slot, value) = tree.take_top();
        assert_eq!(value, 3);
        assert_eq!(tree.peek(), None);

        tree.replace_top(slot, Some(4));
        assert_eq!(tree.peek(), Some(&4));
        assert_eq!(tree.promote_with(None), 4);
        assert_eq!(tree.promote_with(None), 2);
        assert_eq!(tree.promote_with(None), 1);
    }

    #[test]
    fn remove_taken_top() {
        let mut tree = LoserTree::with_capacity(3);
        tree.rebuild_from(vec![3, 2, 1]);

        let (slot, value) = tree.take_top();
        assert_eq!(value, 3);
        tree.replace_top(slot, None);
        assert_eq!(tree.promote_with(None), 2);
        assert_eq!(tree.promote_with(None), 1);
        assert_eq!(tree.peek(), None);
    }
}

struct LoserTree<T: Ord> {
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

        let mut index = self.tree[0].unwrap();
        let mut father = (index + self.data.len()) / 2;
        while father > 0 {
            if let Some(f_index) = self.tree[father] {
                if self.nulls[f_index] {
                    father /= 2;
                    continue;
                }
                if index == self.tree[0].unwrap()
                    || self.nulls[index]
                    || self.data[index] > self.data[f_index]
                {
                    index = f_index;
                }
                father /= 2;
            }
        }
        Some(&self.data[index])
    }

    pub fn peek_mut(&mut self) -> Option<PeekMut<T>> {
        if self.is_empty() {
            None
        } else {
            Some(PeekMut { tree: self })
        }
    }

    fn adjust(&mut self, mut index: usize) {
        let mut father = (index + self.data.len()) / 2;
        while father > 0 {
            match self.tree[father] {
                Some(f_index) => {
                    if self.nulls[f_index] {
                        father /= 2;
                        continue;
                    }
                    if self.nulls[index] || self.data[index] > self.data[f_index] {
                        self.tree[father] = Some(index);
                        index = f_index;
                    }
                    father /= 2;
                }
                None => {
                    self.tree[father] = Some(index);
                    break;
                }
            }
        }
        self.tree[0] = Some(index);
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
            assert_eq!(loser_tree.peek(), Some(i).as_ref());
            assert_eq!(loser_tree.peek(), Some(i).as_ref());
            if i == 9 {
                assert_eq!(loser_tree.peek_top2(), None);
                assert_eq!(loser_tree.peek_top2(), None);
            } else {
                assert_eq!(loser_tree.peek_top2(), Some(i + 1).as_ref());
                assert_eq!(loser_tree.peek_top2(), Some(i + 1).as_ref());
            }
            assert_eq!(loser_tree.pop(), Some(i).as_ref());
        }

        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.len(), 0);
        assert_eq!(loser_tree.peek(), None);
        assert_eq!(loser_tree.peek_top2(), None);
        assert_eq!(loser_tree.is_empty(), true);
    }

    fn basic_odd() {
        let data = vec![4, 6, 5];
        let mut loser_tree = LoserTree::from(data);

        assert_eq!(loser_tree.pop(), Some(4).as_ref());
        assert_eq!(loser_tree.pop(), Some(6).as_ref());
        assert_eq!(loser_tree.pop(), Some(5).as_ref());
        assert_eq!(loser_tree.pop(), None);
        assert_eq!(loser_tree.pop(), None);
    }

    fn peek_mut() {
        let data = vec![2, 3, 7];
        let mut loser_tree = LoserTree::from(data);

        *loser_tree.peek_mut().unwrap() = 4;
        *loser_tree.peek_mut().unwrap() = 5;

        assert_eq!(loser_tree.pop(), Some(4).as_ref());
        assert_eq!(loser_tree.pop(), Some(6).as_ref());
        assert_eq!(loser_tree.pop(), Some(7).as_ref());
        assert_eq!(loser_tree.pop(), None);
    }
}

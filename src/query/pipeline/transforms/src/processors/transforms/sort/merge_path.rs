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

use std::cmp::Ordering;
use std::ops::Range;

pub struct DiagonalIter {
    a: i64,
    b: i64,
    step: i64,
    range: Range<i64>,
}

impl Iterator for DiagonalIter {
    type Item = Diagonal;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|i| Diagonal {
            a: self.a,
            b: self.b,
            offset: i * self.step - self.a,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl ExactSizeIterator for DiagonalIter {}

pub fn create_diagonals(a_len: usize, b_len: usize, parallel: usize) -> DiagonalIter {
    debug_assert!((a_len + b_len) >= parallel && parallel > 0);
    let step = (a_len + b_len) / parallel;
    DiagonalIter {
        a: a_len as i64,
        b: b_len as i64,
        step: step as i64,
        range: 1..parallel as i64,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Diagonal {
    a: i64,
    b: i64,
    offset: i64,
}

impl Diagonal {
    pub fn index(&self, i: usize) -> (usize, usize) {
        if self.offset < 0 {
            (self.len() - 1 - i, i)
        } else {
            (self.a as usize - 1 - i, i + self.offset as usize)
        }
    }

    pub fn len(&self) -> usize {
        if self.offset < 0 {
            self.b.min(self.a + self.offset) as usize
        } else {
            self.a.min(self.b - self.offset) as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn partition_point_by<F>(&self, mut f: F) -> (usize, usize)
    where F: FnMut(usize, usize) -> Ordering {
        // INVARIANTS:
        // - 0 <= left <= left + size = right <= self.len()
        // - f returns a Greater b for everything in self[..left]
        // - f returns a Less b for everything in self[right..]
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            let (a, b) = self.index(mid);
            let cmp = f(a, b);

            left = if cmp == Ordering::Greater {
                mid + 1
            } else {
                left
            };
            right = if cmp == Ordering::Less { mid } else { right };
            if cmp == Ordering::Equal {
                // SAFETY: the while condition means `size` is strictly positive, so
                // `size/2 < size`. Thus `left + size/2 < left + size`, which
                // coupled with the `left + size <= self.len()` invariant means
                // we have `left + size/2 < self.len()`, and this is in-bounds.
                // unsafe { std::hint::assert_unchecked(mid < self.len()) };
                let (a, b) = self.index(mid);
                return (a + 1, b);
            }

            size = right - left;
        }

        // SAFETY: directly true from the overall invariant.
        // Note that this is `<=`, unlike the assume in the `Ok` path.
        // unsafe { std::hint::assert_unchecked(left <= self.len()) };
        if self.offset < 0 {
            (self.len() - left, left)
        } else {
            (self.a as usize - left, left + self.offset as usize)
        }
    }

    pub fn partition_point<T>(&self, a: &[T], b: &[T]) -> (usize, usize)
    where T: Ord {
        debug_assert_eq!(self.a as usize, a.len());
        debug_assert_eq!(self.b as usize, b.len());
        self.partition_point_by(|i, j| a[i].cmp(&b[j]))
    }

    pub fn domain(&self) -> (Range<usize>, Range<usize>) {
        let (a0, b0) = self.index(0);
        let (a1, b1) = self.index(self.len() - 1);
        (a1..a0 + 1, b0..b1 + 1)
    }
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use super::*;

    #[test]
    fn test_diagonals() {
        let a = vec![17, 29, 35, 73, 86, 90, 95, 99];
        let b = vec![3, 5, 12, 22, 45, 64, 69, 82];
        println!("\n#1");
        run_test(&a, &b, 4);
        println!("\n#2");
        run_test(&a, &b, 3);
        println!("\n#3");
        run_test(&a, &b, 1);
        println!("\n#4");
        run_test(&a, &b, 16);
        let a = vec![17, 29, 35, 73];
        let b = vec![3, 5, 12, 22, 45, 64, 69, 82];
        println!("\n#5");
        run_test(&a, &b, 4);
        println!("\n#6");
        run_test(&a, &b, 3);
        let a = vec![1, 1, 2, 2, 3];
        let b = vec![2, 2, 3, 4];
        println!("\n#7");
        run_test(&a, &b, 3);
        println!("\n#8");
        run_test(&a, &b, 7);
    }

    fn run_test(a: &[i32], b: &[i32], parallel: usize) {
        once((0, 0))
            .chain(create_diagonals(a.len(), b.len(), parallel).map(|d| d.partition_point(a, b)))
            .chain(once((a.len(), b.len())))
            .map_windows(|&[start, end]| {
                let pa = &a[start.0..end.0];
                let pb = &b[start.1..end.1];
                println!("a {:?} b {:?}", pa, pb);

                let mut all = Vec::new();
                all.extend_from_slice(pa);
                all.extend_from_slice(pb);

                (all.iter().copied().min(), all.iter().copied().max())
            })
            .map_windows(|&[previous, next]| assert!(previous.1 <= next.0))
            .for_each(|_| ());
    }
}

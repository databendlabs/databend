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
use std::collections::BTreeSet;
use std::fmt::Debug;

pub trait List
where Self: Debug
{
    type Item<'a>: Ord + Debug
    where Self: 'a;

    fn len(&self) -> usize;
    fn cmp_value<'a>(&'a self, i: usize, target: &Self::Item<'a>) -> Ordering;
    fn index(&self, i: usize) -> Self::Item<'_>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first(&self) -> Option<Self::Item<'_>> {
        if self.is_empty() {
            None
        } else {
            Some(self.index(0))
        }
    }

    fn last(&self) -> Option<Self::Item<'_>> {
        if self.is_empty() {
            None
        } else {
            Some(self.index(self.len() - 1))
        }
    }

    fn domain(&self) -> EndDomain {
        EndDomain {
            min: 0,
            max: self.len(),
        }
    }

    fn search<'a>(&'a self, target: &Self::Item<'a>, domain: EndDomain) -> EndDomain {
        if domain.done() {
            return domain;
        }
        let mid = domain.mid();
        if self.cmp_value(mid, target) == Ordering::Greater {
            EndDomain {
                min: domain.min,
                max: mid,
            }
        } else {
            EndDomain {
                min: mid + 1,
                max: domain.max,
            }
        }
    }
}

#[derive(Debug)]
pub struct Partition {
    pub ends: Vec<(usize, usize)>, // index, partition point
    pub total: usize,
}

impl Partition {
    fn new<T>(item: TargetItem<'_, T>) -> Self
    where T: List {
        let TargetItem { domains, sum, .. } = item;
        debug_assert!(sum.done());

        Self {
            ends: domains
                .iter()
                .enumerate()
                .filter_map(|(i, domain)| {
                    debug_assert!(domain.done());
                    if domain.is_zero() {
                        None
                    } else {
                        Some((i, domain.min))
                    }
                })
                .collect(),
            total: sum.min,
        }
    }
}

pub struct Candidate<'a, T>
where T: List
{
    all_list: &'a [T],
    expect: EndDomain,
    min_target: Option<TargetItem<'a, T>>,
    mid_target: Option<TargetItem<'a, T>>,
    max_target: Option<TargetItem<'a, T>>,
}

struct TargetItem<'a, T>
where
    T: List + 'a,
    T::Item<'a>: Debug,
{
    // The cut point value.
    target: T::Item<'a>,
    // The domain of partition point in each list, and also the domain of the generated task size in each list.
    domains: Vec<EndDomain>,
    // The size domain of the generated task if the current target is used as the cut point.
    sum: EndDomain,
}

impl<'a, T> Candidate<'a, T>
where T: List
{
    pub fn new(all_list: &'a [T], expect: EndDomain) -> Self {
        Self {
            all_list,
            expect,
            min_target: None,
            mid_target: None,
            max_target: None,
        }
    }

    pub fn init(&mut self) -> bool {
        // Take the smallest first and the smallest last of all the lists as the initial target range.
        let target: (Option<T::Item<'a>>, Option<T::Item<'a>>) =
            self.all_list.iter().fold((None, None), |(min, max), ls| {
                let min = match (min, ls.first()) {
                    (Some(acc), Some(v)) => Some(acc.min(v)),
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (None, None) => None,
                };
                let max = match (max, ls.last()) {
                    (Some(acc), Some(v)) => Some(acc.min(v)),
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (None, None) => None,
                };

                (min, max)
            });
        let (Some(min_target), Some(max_target)) = target else {
            // invalid empty input
            return false;
        };

        let domains = self
            .all_list
            .iter()
            .map(|ls| {
                ls.first().map_or(EndDomain::default(), |first| {
                    if first > max_target {
                        EndDomain::default()
                    } else {
                        ls.domain()
                    }
                })
            })
            .collect::<Vec<_>>();
        let sum: EndDomain = domains.iter().copied().sum();

        self.max_target = Some(TargetItem {
            target: max_target,
            domains: domains.clone(),
            sum,
        });

        self.min_target = Some(TargetItem {
            target: min_target,
            domains: domains.clone(),
            sum,
        });

        true
    }

    #[allow(dead_code)]
    pub fn is_small_task(&mut self) -> bool {
        loop {
            let sum = self.reduce_max_domain(Some(8));
            match self.expect.overlaps(sum) {
                Overlap::Left => return true,
                Overlap::Right => return false,
                Overlap::Cross if sum.done() => return false,
                Overlap::Cross => (),
            }
        }
    }

    pub fn calc_partition(mut self, n: usize, max_iter: usize) -> Partition {
        for _ in 0..max_iter {
            match self.overlaps() {
                (_, _, Overlap::Cross) => {
                    let sum = self.reduce_max_domain(Some(n));
                    if self.is_finish(sum) {
                        return Partition::new(self.max_target.unwrap());
                    }
                }
                (_, _, Overlap::Left) => break,
                (_, None, Overlap::Right) => {
                    if let Some(target) = self.find_target() {
                        self.update_mid(target);
                    } else {
                        break;
                    }
                }
                (
                    min_overlap @ (Overlap::Cross | Overlap::Left),
                    Some(Overlap::Cross),
                    Overlap::Right,
                ) => {
                    let sum = self.reduce_mid_domain(Some(n));
                    match self.expect.overlaps(sum) {
                        Overlap::Right => self.cut_right(),
                        Overlap::Left if matches!(min_overlap, Overlap::Left) => self.cut_left(),
                        Overlap::Cross if sum.done() => {
                            return Partition::new(self.mid_target.unwrap());
                        }
                        Overlap::Cross | Overlap::Left => (),
                    }
                }
                (Overlap::Cross, Some(Overlap::Left), Overlap::Right) => {
                    let sum = self.reduce_min_domain(Some(n));
                    match self.expect.overlaps(sum) {
                        Overlap::Left => self.cut_left(),
                        Overlap::Cross if sum.done() => {
                            return Partition::new(self.min_target.unwrap());
                        }
                        Overlap::Cross | Overlap::Right => (),
                    }
                }
                x => {
                    if cfg!(debug_assertions) {
                        unreachable!("unreachable {x:?}");
                    } else {
                        break;
                    }
                }
            };
        }

        self.reduce_max_domain(None);
        Partition::new(self.max_target.unwrap())
    }

    fn reduce_max_domain(&mut self, n: Option<usize>) -> EndDomain {
        do_search(self.all_list, self.max_target.as_mut().unwrap(), n)
    }

    fn reduce_min_domain(&mut self, n: Option<usize>) -> EndDomain {
        do_search(self.all_list, self.min_target.as_mut().unwrap(), n)
    }

    fn reduce_mid_domain(&mut self, n: Option<usize>) -> EndDomain {
        do_search(self.all_list, self.mid_target.as_mut().unwrap(), n)
    }

    fn find_target<'b>(&'b self) -> Option<T::Item<'a>> {
        let TargetItem {
            target: min_target,
            domains: min_domains,
            ..
        } = self.min_target.as_ref().unwrap();

        let TargetItem {
            target: max_target,
            domains: max_domains,
            ..
        } = self.max_target.as_ref().unwrap();

        let mut targets = BTreeSet::new();

        for ((min_domain, max_domain), ls) in min_domains
            .iter()
            .zip(max_domains.iter())
            .zip(self.all_list.iter())
        {
            if max_domain.is_zero() {
                continue;
            }

            let five = min_domain.merge(max_domain).five_point();
            for v in five.into_iter().filter_map(|i| {
                let v = ls.index(i);
                if v >= *min_target && v <= *max_target {
                    Some(v)
                } else {
                    None
                }
            }) {
                targets.insert(v);
            }
        }

        let n = targets.len();
        targets.into_iter().nth(n / 2)
    }

    fn update_mid(&mut self, target: T::Item<'a>) {
        let max = self.max_target.as_ref().unwrap();

        let domains = max
            .domains
            .iter()
            .map(|domain| EndDomain {
                min: 0,
                max: domain.max,
            })
            .collect::<Vec<_>>();
        let sum: EndDomain = domains.iter().copied().sum();

        self.mid_target = Some(TargetItem {
            target,
            domains,
            sum,
        })
    }

    fn is_finish(&self, domain: EndDomain) -> bool {
        domain.done() && matches!(self.expect.overlaps(domain), Overlap::Cross)
    }

    fn overlaps(&self) -> (Overlap, Option<Overlap>, Overlap) {
        // Compare expect task size domain with min_target,mid_target and max_target task size domain.
        (
            self.expect.overlaps(self.min_target.as_ref().unwrap().sum),
            self.mid_target
                .as_ref()
                .map(|item| self.expect.overlaps(item.sum)),
            self.expect.overlaps(self.max_target.as_ref().unwrap().sum),
        )
    }

    fn cut_left(&mut self) {
        self.min_target = self.mid_target.take()
    }

    fn cut_right(&mut self) {
        self.max_target = self.mid_target.take()
    }
}

fn do_search<'a, T>(
    all_list: &'a [T],
    item: &mut TargetItem<'a, T>,
    n: Option<usize>,
) -> EndDomain
where
    T: List + 'a,
{
    let TargetItem {
        target,
        domains,
        sum,
    } = item;

    domains
        .iter_mut()
        .zip(all_list.iter())
        .for_each(|(domain, ls)| match n {
            Some(n) => {
                for _ in 0..n {
                    if domain.done() {
                        break;
                    }
                    *domain = ls.search(target, *domain)
                }
            }
            None => {
                while !domain.done() {
                    *domain = ls.search(target, *domain)
                }
            }
        });
    *sum = domains.iter().copied().sum();
    *sum
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EndDomain {
    min: usize,
    max: usize,
}

impl EndDomain {
    pub fn new(min: usize, max: usize) -> EndDomain {
        assert!(min <= max);
        EndDomain { min, max }
    }

    fn done(&self) -> bool {
        self.min == self.max
    }

    fn size(&self) -> usize {
        self.max - self.min
    }

    fn overlaps(&self, rhs: Self) -> Overlap {
        if rhs.max < self.min {
            return Overlap::Left;
        }
        if rhs.min > self.max {
            return Overlap::Right;
        }
        Overlap::Cross
    }

    fn left_half(&self) -> EndDomain {
        EndDomain {
            min: self.min,
            max: self.mid(),
        }
    }

    fn right_half(&self) -> EndDomain {
        EndDomain {
            min: self.mid() + 1,
            max: self.max,
        }
    }

    fn mid(&self) -> usize {
        self.min + self.size() / 2
    }

    fn is_zero(&self) -> bool {
        *self == EndDomain::default()
    }

    fn five_point(&self) -> Vec<usize> {
        match self.size() {
            0 => vec![],
            1 => vec![self.min],
            2 => vec![self.min, self.max - 1],
            3 => vec![self.min, self.min + 1, self.max - 1],
            4 => vec![self.min, self.min + 1, self.min + 2, self.max - 1],
            _ => vec![
                self.min,
                self.left_half().mid(),
                self.mid(),
                self.right_half().mid(),
                self.max - 1,
            ],
        }
    }

    fn merge(&self, other: &EndDomain) -> EndDomain {
        EndDomain {
            min: self.min.min(other.min),
            max: self.max.max(other.max),
        }
    }
}

#[derive(Debug)]
enum Overlap {
    Left,
    Cross,
    Right,
}

impl std::ops::Add for EndDomain {
    type Output = EndDomain;

    fn add(self, rhs: Self) -> Self::Output {
        EndDomain {
            min: self.min + rhs.min,
            max: self.max + rhs.max,
        }
    }
}

impl std::iter::Sum for EndDomain {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.reduce(|acc, v| acc + v).unwrap_or_default()
    }
}

impl From<std::ops::RangeInclusive<usize>> for EndDomain {
    fn from(value: std::ops::RangeInclusive<usize>) -> Self {
        EndDomain::new(*value.start(), *value.end())
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    impl List for &[i32] {
        type Item<'a>
            = &'a i32
        where Self: 'a;

        fn cmp_value(&self, i: usize, target: &&i32) -> Ordering {
            self[i].cmp(target)
        }

        fn len(&self) -> usize {
            (*self).len()
        }

        fn index(&self, i: usize) -> &i32 {
            &self[i]
        }
    }

    #[test]
    fn test_calc_partition() {
        {
            let all_list: Vec<Vec<i32>> = vec![vec![]];
            let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();
            run_test(&all_list, (5..=10).into(), 10);
        }

        {
            let all_list: Vec<Vec<i32>> = vec![
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5],
            ];
            let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();
            run_test(&all_list, (5..=10).into(), 10);
        }

        {
            let all_list = issue_16923();

            let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();
            run_test(&all_list, (5..=100).into(), 20);
        }

        for _ in 0..100 {
            let all_list = rand_data(rand::random());
            let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();

            run_test(&all_list, (5..=100).into(), 20)
        }
    }

    fn rand_data(seed: u64) -> Vec<Vec<i32>> {
        let mut rng = StdRng::seed_from_u64(seed);

        let list = rng.gen_range(1..=10);
        repeat_with(|| {
            let rows = rng.gen_range(0..=40);
            let mut data = repeat_with(|| rng.gen_range(0..=1000))
                .take(rows)
                .collect::<Vec<_>>();
            data.sort();
            data
        })
        .take(list)
        .collect::<Vec<_>>()
    }

    fn issue_16923() -> Vec<Vec<i32>> {
        vec![
            vec![6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6],
            vec![
                3, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 8, 13, 13, 13, 13, 13, 13,
                13, 13, 13, 13, 13, 13, 13, 13, 13, 18,
            ],
            vec![6, 6, 6, 6, 6],
            vec![
                2, 4, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 11, 12, 14, 15, 16, 19,
            ],
            vec![
                6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
            ],
            vec![
                1, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                11, 12, 14, 15, 17, 18, 21, 22, 24, 25, 27,
            ],
            vec![
                0, 9, 10, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 20,
                23, 26, 27, 27, 27, 27, 27, 27, 27, 28,
            ],
        ]
    }

    fn run_test(all_list: &[&[i32]], expect_size: EndDomain, max_iter: usize) {
        let mut candidate = Candidate::new(all_list, expect_size);

        let got = if candidate.init() {
            candidate.calc_partition(4, max_iter)
        } else {
            let sum: usize = all_list.iter().map(|ls| ls.len()).sum();
            assert_eq!(sum, 0);
            return;
        };

        // println!("total {}", got.total);

        let sum: usize = got.ends.iter().map(|(_, end)| *end).sum();
        assert_eq!(sum, got.total, "all_list {all_list:?}");

        let x = got
            .ends
            .iter()
            .copied()
            .map(|(i, end)| {
                let ls = all_list[i];
                (ls[..end].last(), ls[end..].first())
            })
            .fold((None, None), |acc, (end, start)| {
                let max_end = acc.0.max(end);
                let min_start = match (acc.1, start) {
                    (None, None) => None,
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (Some(a), Some(b)) => Some(a.min(b)),
                };
                (max_end, min_start)
            });
        match x {
            (Some(a), Some(b)) => assert!(a < b, "all_list {all_list:?}"),
            (None, None) => unreachable!(),
            _ => (),
        }
    }
}

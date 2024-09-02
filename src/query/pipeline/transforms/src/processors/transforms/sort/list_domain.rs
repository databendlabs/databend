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
use std::ops::Index;

pub trait List<T>
where
    T: Ord,
    Self: Index<usize, Output = T> + Debug,
{
    fn len(&self) -> usize;
    fn cmp_value(&self, i: usize, target: &T) -> Ordering;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[0])
        }
    }

    fn last(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[self.len() - 1])
        }
    }

    fn max_iter(&self, target: &T, n: usize) -> EndDomain {
        binary_search(self, target, Some(n.into()), None)
    }

    fn max_diff(&self, target: &T, diff: usize) -> EndDomain {
        binary_search(self, target, None, Some(diff))
    }

    fn index_gt(&self, target: &T) -> usize {
        binary_search(self, target, None, Some(0)).min
    }

    fn domain(&self) -> EndDomain {
        EndDomain {
            min: 0,
            max: self.len(),
        }
    }

    fn search(&self, target: &T, domain: EndDomain) -> EndDomain {
        if domain.done() {
            return domain;
        }
        let mid = domain.mid();
        return if self.cmp_value(mid, target) == Ordering::Greater {
            EndDomain {
                min: domain.min,
                max: mid,
            }
        } else {
            EndDomain {
                min: mid + 1,
                max: domain.max,
            }
        };
    }
}

fn binary_search<T, U>(ls: &U, target: &T, mut n: Option<usize>, diff: Option<usize>) -> EndDomain
where
    T: Ord,
    U: List<T> + ?Sized,
{
    // INVARIANTS:
    // - 0 <= left <= left + size = right <= self.len()
    // - f returns Less for everything in self[..left]
    // - f returns Greater for everything in self[right..]
    let mut size = ls.len();
    let mut domain = EndDomain { min: 0, max: size };
    while domain.min < domain.max {
        if let Some(n) = &mut n {
            if *n == 0 {
                return domain;
            } else {
                *n -= 1
            }
        }
        if let Some(diff) = diff {
            if size <= diff {
                return domain;
            }
        }

        let mid = domain.min + size / 2;

        domain = if ls.cmp_value(mid, target) == Ordering::Greater {
            EndDomain {
                min: domain.min,
                max: mid,
            }
        } else {
            EndDomain {
                min: mid + 1,
                max: domain.max,
            }
        };

        size = domain.max - domain.min;
    }

    // SAFETY: directly true from the overall invariant.
    // Note that this is `<=`, unlike the assume in the `Ok` path.
    // unsafe { std::hint::assert_unchecked(left <= self.len()) };
    domain
}

#[derive(Debug)]
pub struct Partition {
    pub ends: Vec<(usize, usize)>,
    pub total: usize,
}

pub fn calc_partition<T, U>(all_list: &[&U], expect_size: EndDomain) -> Option<Partition>
where
    T: Ord + Debug,
    U: List<T> + ?Sized,
{
    let mut candidate = Candidate::new(all_list, expect_size);

    if !candidate.init() {
        return None;
    }

    println!("begen");

    for _ in 0..10 {
        for (i, item) in [
            &candidate.min_target.as_ref(),
            &candidate.mid_target.as_ref(),
            &candidate.max_target.as_ref(),
        ]
        .iter()
        .copied()
        .enumerate()
        {
            if let Some(Item {
                target: k,
                domains,
                sum,
            }) = item
            {
                println!("{i} k {k:?}  {},{} domains {domains:?}", sum.min, sum.max);
            }
        }
        println!();

        macro_rules! overlap_cross {
            () => {
                Overlap::LeftCross | Overlap::Full | Overlap::RightCross
            };
        }

        match candidate.status() {
            (_, None, overlap_cross!()) => {
                let sum = candidate.do_search_max();
                if candidate.aaa(sum) {
                    println!("end");
                    break;
                }
            }
            (_, None, Overlap::Right) => {
                // let sum = candidate.do_search_max();
                // if sum.done() {
                //     break;
                // }

                // if !candidate.expect.overlaps(sum).is_disjoint() {}

                // if matches!(
                //     candidate.expect.overlaps(sum),
                //     Overlap::LeftCross | Overlap::Full | Overlap::RightCross
                // ) {
                //     continue;
                // }

                let set = candidate.find_target();
                let mid = set.len() / 2;
                debug_assert!(mid != 0);
                let target = set.iter().nth(mid).unwrap();
                println!("new target {target:?}");
                candidate.update_mid(target);

                candidate.do_search_mid();
                println!("");

                // matches!()
            }
            (_, Some(overlap_cross!()), Overlap::Right) => {
                let sum = candidate.do_search_mid();
                if candidate.aaa(sum) {
                    println!("end");
                    break;
                }
            }
            (_, Some(Overlap::Right), Overlap::Right) => {
                println!("cut_right");
                candidate.cut_right()
            }
            (overlap_cross!(), Some(Overlap::Left), Overlap::Right) => {
                let sum = candidate.do_search_min();
                if candidate.aaa(sum) {
                    println!("end");
                    break;
                }
            }
            (Overlap::Left, Some(Overlap::Left), Overlap::Right) => {
                println!("cut_left");
                candidate.cut_left()
            }

            _ => todo!(),
        };
    }

    None
}

struct Candidate<'a, T, U>
where
    T: Ord + Debug,
    U: List<T> + ?Sized,
{
    all_list: &'a [&'a U],
    expect: EndDomain,
    min_target: Option<Item<'a, T>>,
    mid_target: Option<Item<'a, T>>,
    max_target: Option<Item<'a, T>>,
}

struct Item<'a, T> {
    target: &'a T,
    domains: Vec<EndDomain>,
    sum: EndDomain,
}

impl<'a, T, U> Candidate<'a, T, U>
where
    T: Ord + Debug,
    U: List<T> + ?Sized,
{
    fn new(all_list: &'a [&'a U], expect: EndDomain) -> Self {
        Self {
            all_list,
            expect,
            min_target: None,
            mid_target: None,
            max_target: None,
        }
    }

    fn init(&mut self) -> bool {
        let max_target: Option<&T> =
            self.all_list
                .iter()
                .fold(None, |acc, ls| match (acc, ls.last()) {
                    (Some(acc), Some(last)) => Some(acc.min(last)),
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (None, None) => None,
                });
        let max_target = if let Some(v) = max_target {
            v
        } else {
            return false;
        };

        let min_target: Option<&T> =
            self.all_list
                .iter()
                .fold(None, |acc, ls| match (acc, ls.first()) {
                    (Some(acc), Some(last)) => Some(acc.min(last)),
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (None, None) => None,
                });

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

        self.max_target = Some(Item {
            target: max_target,
            domains: domains.clone(),
            sum,
        });

        self.min_target = Some(Item {
            target: min_target.unwrap(),
            domains: domains.clone(),
            sum,
        });

        true
    }

    fn do_search_max(&mut self) -> EndDomain {
        do_search(self.all_list, self.max_target.as_mut().unwrap())
    }

    fn do_search_min(&mut self) -> EndDomain {
        do_search(self.all_list, self.min_target.as_mut().unwrap())
    }

    fn do_search_mid(&mut self) -> EndDomain {
        do_search(self.all_list, self.mid_target.as_mut().unwrap())
    }

    fn find_target<'b>(&'b self) -> BTreeSet<&'a T> {
        let Item {
            target: min_target,
            domains: min_domains,
            ..
        } = self.min_target.as_ref().unwrap();

        let Item {
            target: max_target,
            domains: max_domains,
            ..
        } = self.max_target.as_ref().unwrap();

        let mut targets_set = BTreeSet::new();

        for ((min_domain, max_domain), ls) in min_domains
            .iter()
            .zip(max_domains.iter())
            .zip(self.all_list.iter())
        {
            if max_domain.is_zero() {
                continue;
            }
            let five = EndDomain {
                min: min_domain.min,
                max: max_domain.min,
            }
            .five_point();
            for v in five.into_iter().filter_map(|i| {
                let v = &ls[i];
                if v > min_target && v < max_target {
                    Some(v)
                } else {
                    None
                }
            }) {
                targets_set.insert(v);
            }
        }

        targets_set
    }

    fn update_mid(&mut self, target: &'a T) {
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

        self.mid_target = Some(Item {
            target,
            domains,
            sum,
        })
    }

    fn status(&self) -> (Overlap, Option<Overlap>, Overlap) {
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

    fn aaa(&self, domain: EndDomain) -> bool {
        domain.done() && !self.expect.overlaps(domain).is_disjoint()
    }
}

fn do_search<'a, T, U>(all_list: &[&U], item: &mut Item<'a, T>) -> EndDomain
where
    T: Ord + Debug,
    U: List<T> + ?Sized,
{
    let Item {
        target,
        domains,
        sum,
    } = item;

    domains
        .iter_mut()
        .zip(all_list.iter())
        .for_each(|(domain, ls)| *domain = ls.search(target, *domain));
    *sum = domains.iter().copied().sum();
    *sum
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EndDomain {
    pub min: usize,
    pub max: usize,
}

impl EndDomain {
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

        match (rhs.min < self.min, rhs.max > self.max) {
            (true, true) => Overlap::Full,
            (true, false) => Overlap::LeftCross,
            (false, true) => Overlap::RightCross,
            (false, false) => Overlap::Full,
        }
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
}

#[derive(Debug)]
enum Overlap {
    Left,
    LeftCross,
    Full,
    RightCross,
    Right,
}

impl Overlap {
    fn is_disjoint(&self) -> bool {
        match &self {
            Overlap::Right | Overlap::Left => true,
            Overlap::LeftCross | Overlap::RightCross | Overlap::Full => false,
        }
    }
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
        if let Some(domain) = iter.reduce(|acc, v| acc + v) {
            domain
        } else {
            EndDomain::default()
        }
    }
}

impl From<std::ops::RangeInclusive<usize>> for EndDomain {
    fn from(value: std::ops::RangeInclusive<usize>) -> Self {
        EndDomain {
            min: *value.start(),
            max: *value.end(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl List<i32> for [i32] {
        fn cmp_value(&self, i: usize, target: &i32) -> Ordering {
            self[i].cmp(target)
        }

        fn len(&self) -> usize {
            self.len()
        }
    }

    #[test]
    fn test_binary_search() {
        let ls = &[1, 2, 3, 4, 4, 4, 5, 6, 7, 7, 8];

        assert_eq!(0, ls.index_gt(&-1));
        assert_eq!(6, ls.index_gt(&4));
        assert_eq!(2, ls.index_gt(&2));
        assert_eq!(11, ls.index_gt(&100));

        assert_eq!(EndDomain { min: 0, max: 11 }, ls.max_diff(&4, 100));
        assert_eq!(EndDomain { min: 6, max: 11 }, ls.max_diff(&4, 5));
        assert_eq!(EndDomain { min: 6, max: 8 }, ls.max_diff(&4, 2));
        assert_eq!(EndDomain { min: 6, max: 6 }, ls.max_diff(&4, 0));

        assert_eq!(EndDomain { min: 0, max: 11 }, ls.max_iter(&1, 0));
        assert_eq!(EndDomain { min: 0, max: 5 }, ls.max_iter(&1, 1));
        assert_eq!(EndDomain { min: 0, max: 2 }, ls.max_iter(&1, 2));
        assert_eq!(EndDomain { min: 6, max: 6 }, ls.max_iter(&4, 100));
    }

    #[test]
    fn test_calc_partition() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let all_list = (0..5)
            .map(|_| {
                let rows: usize = rng.gen_range(0..=20);
                let mut data = (0..rows)
                    .map(|_| rng.gen_range(0..=1000))
                    .collect::<Vec<_>>();
                data.sort();
                data
            })
            .collect::<Vec<_>>();

        // let all_list = vec![
        //     vec![1, 2, 3, 4],
        //     vec![10, 100, 2000],
        //     vec![4, 5, 6, 7],
        //     vec![0, 2, 4, 5],
        //     // vec![1, 1, 1, 1],
        // ];

        let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();

        let x = calc_partition(&all_list, (5..=10).into());
    }
}

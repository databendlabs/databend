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

#[derive(Debug)]
pub struct Partition {
    pub ends: Vec<(usize, usize)>,
    pub total: usize,
}

impl Partition {
    fn new<'a, T>(item: Item<'a, T>) -> Self {
        let Item { domains, sum, .. } = item;
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

pub fn calc_partition<T, U>(
    all_list: &[&U],
    expect_size: EndDomain,
    max_iter: usize,
) -> Option<Partition>
where
    T: Ord + Debug,
    U: List<T> + ?Sized,
{
    let mut candidate = Candidate::new(all_list, expect_size);

    if !candidate.init() {
        return None;
    }

    for _ in 0..max_iter {
        match candidate.overlaps() {
            (_, _, Overlap::Cross) => {
                let sum = candidate.do_search_max();
                if candidate.is_finish(sum) {
                    return Some(Partition::new(candidate.max_target.unwrap()));
                }
            }
            (_, _, Overlap::Left) => break,
            (_, None, Overlap::Right) => {
                let target = candidate.find_target();
                let target = if target.is_none() {
                    break;
                } else {
                    target.unwrap()
                };

                candidate.update_mid(target);
            }

            (
                min_overlap @ (Overlap::Cross | Overlap::Left),
                Some(Overlap::Cross),
                Overlap::Right,
            ) => {
                let sum = candidate.do_search_mid();

                match candidate.expect.overlaps(sum) {
                    Overlap::Right => candidate.cut_right(),
                    Overlap::Left if matches!(min_overlap, Overlap::Left) => candidate.cut_left(),
                    Overlap::Cross if sum.done() => {
                        return Some(Partition::new(candidate.mid_target.unwrap()));
                    }
                    Overlap::Cross | Overlap::Left => (),
                }
            }
            (Overlap::Cross, Some(Overlap::Left), Overlap::Right) => {
                let sum = candidate.do_search_min();
                if candidate.is_finish(sum) {
                    return Some(Partition::new(candidate.min_target.unwrap()));
                }
            }
            x => {
                debug_assert!(true, "unreachable {x:?}");
                break;
            }
        };
    }

    loop {
        let sum = candidate.do_search_max();
        if sum.done() {
            return Some(Partition::new(candidate.max_target.unwrap()));
        }
    }
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
        let target: (Option<&T>, Option<&T>) =
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
        let (min_target, max_target) = if let (Some(min), Some(max)) = target {
            (min, max)
        } else {
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

        self.max_target = Some(Item {
            target: max_target,
            domains: domains.clone(),
            sum,
        });

        self.min_target = Some(Item {
            target: min_target,
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

    fn find_target<'b>(&'b self) -> Option<&'a T> {
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

        let mut targets = BTreeSet::new();

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
                if v >= min_target && v <= max_target {
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

    fn is_finish(&self, domain: EndDomain) -> bool {
        domain.done() && matches!(self.expect.overlaps(domain), Overlap::Cross)
    }

    fn overlaps(&self) -> (Overlap, Option<Overlap>, Overlap) {
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

        return Overlap::Cross;
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

        for _ in 0..100 {
            let all_list = rand_data();
            let all_list: Vec<_> = all_list.iter().map(|v| v.as_slice()).collect();

            run_test(&all_list, (5..=10).into(), 10)
        }
    }

    fn rand_data() -> Vec<Vec<i32>> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        (0..5)
            .map(|_| {
                let rows: usize = rng.gen_range(0..=20);
                let mut data = (0..rows)
                    .map(|_| rng.gen_range(0..=1000))
                    .collect::<Vec<_>>();
                data.sort();
                data
            })
            .collect::<Vec<_>>()
    }

    fn run_test(all_list: &[&[i32]], expect_size: EndDomain, max_iter: usize) {
        let got = calc_partition(all_list, expect_size, max_iter);

        let got = if got.is_none() {
            let sum: usize = all_list.iter().map(|ls| ls.len()).sum();
            assert_eq!(sum, 0);

            return;
        } else {
            got.unwrap()
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
                (acc.0.max(end), match (acc.1, start) {
                    (None, None) => None,
                    (None, v @ Some(_)) | (v @ Some(_), None) => v,
                    (Some(a), Some(b)) => Some(a.min(b)),
                })
            });
        match x {
            (Some(a), Some(b)) => assert!(a < b, "all_list {all_list:?}"),
            (None, None) => unreachable!(),
            _ => (),
        }
    }
}

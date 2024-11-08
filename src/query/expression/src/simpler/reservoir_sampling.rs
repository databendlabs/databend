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

use rand::Rng;

/// An implementation of Algorithm `L` (https://en.wikipedia.org/wiki/Reservoir_sampling#An_optimal_algorithm)
pub struct AlgoL<'a, R: Rng + ?Sized> {
    k: usize,
    r: &'a mut R,

    i: usize,
    w: f64,
}

impl<R: Rng + ?Sized> AlgoL<'_, R> {
    pub fn new<'a>(k: usize, rng: &'a mut R) -> AlgoL<'a, R> {
        assert!(k > 0);
        let mut al = AlgoL::<'a, R> {
            k,
            i: k - 1,
            w: 1.0,
            r: rng,
        };
        al.update_w();
        al
    }

    pub fn next_index(&mut self) -> usize {
        let i = (self.rng().log2() / (1.0 - self.w).log2()).floor() + 1.0 + self.i as f64;
        if i.is_normal() && i < u64::MAX as f64 {
            i as usize
        } else {
            usize::MAX
        }
    }

    pub fn pos(&mut self) -> usize {
        self.r.sample(rand::distributions::Uniform::new(0, self.k))
    }

    pub fn update(&mut self, i: usize) {
        self.i = i;
        self.update_w()
    }

    fn rng(&mut self) -> f64 {
        self.r.sample(rand::distributions::Open01)
    }

    fn update_w(&mut self) {
        self.w *= (self.rng().log2() / self.k as f64).exp2(); // rng ^ (1/k)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_algo_l() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut sample = vec![0_u64; 10];

        let mut al = AlgoL::new(10, &mut rng);
        for (i, v) in sample.iter_mut().enumerate() {
            *v = i as u64
        }

        loop {
            let i = al.next_index();
            if i < 100 {
                sample[al.pos()] = i as u64;
                al.update(i)
            } else {
                break;
            }
        }

        let want: Vec<u64> = vec![69, 49, 53, 83, 4, 72, 88, 38, 45, 27];
        assert_eq!(want, sample)
    }
}

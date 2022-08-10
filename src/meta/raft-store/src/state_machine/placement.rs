// Copyright 2021 Datafuse Labs.
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

/// Evenly chooses `n` elements from `m` elements
pub fn rand_n_from_m(m: usize, n: usize) -> anyhow::Result<Vec<usize>> {
    if m < n {
        return Err(anyhow::anyhow!("m={} must >= n={}", m, n));
    }

    let mut chosen = Vec::with_capacity(n);

    let mut need = n;
    for i in 0..m {
        if rand::random::<usize>() % (m - i) < need {
            chosen.push(i);
            need -= 1;
        }
    }

    Ok(chosen)
}

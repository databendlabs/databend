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

use common_meta_raft_store::state_machine::placement::rand_n_from_m;

#[test]
fn test_rand_n_from_m() -> anyhow::Result<()> {
    // - Randomly choose n elts from total elts.
    // - Assert that the count of the result.
    // - Assert every elt is different.

    for total in 1..10 {
        for n in 0..total {
            let got = rand_n_from_m(total, n)?;
            assert_eq!(n, got.len());

            if !got.is_empty() {
                let mut prev = got[0];
                for v in got.iter().skip(1) {
                    assert_ne!(prev, *v);
                    prev = *v;
                }
            }
        }
    }

    Ok(())
}

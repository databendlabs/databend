// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::meta_service::placement::rand_n_from_m;

#[test]
fn test_rand_n_from_m() -> anyhow::Result<()> {
    // - Randomly choose n elts from total elts.
    // - Assert that the count of the result.
    // - Assert every elt is different.

    for total in 1..10 {
        for n in 0..total {
            let got = rand_n_from_m(total, n)?;
            assert_eq!(n, got.len());

            if got.len() > 0 {
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

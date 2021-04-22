// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_util_generate_parts() -> anyhow::Result<()> {
    use common_planners::Partition;
    use pretty_assertions::assert_eq;

    use crate::datasources::util::*;

    {
        // deal with remainder
        let ps = generate_parts(0, 3, 11);

        assert_eq!(3, ps.len());
        assert_eq!(
            Partition {
                name: "11-0-3".into(),
                version: 0,
            },
            ps[0]
        );
        assert_eq!(
            Partition {
                name: "11-3-6".into(),
                version: 0,
            },
            ps[1]
        );
        assert_eq!(
            Partition {
                name: "11-6-11".into(),
                version: 0,
            },
            ps[2]
        );
    }

    {
        // total is zero
        let ps = generate_parts(0, 3, 0);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "0-0-0".into(),
                version: 0,
            },
            ps[0]
        );
    }
    {
        // only one part, total < workers
        let ps = generate_parts(0, 3, 2);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "2-0-2".into(),
                version: 0,
            },
            ps[0]
        );
    }

    Ok(())
}

#[test]
fn test_lines_count() -> anyhow::Result<()> {
    use std::env;

    use pretty_assertions::assert_eq;

    use crate::datasources::util::*;

    let file = env::current_dir()?
        .join("../../tests/data/sample.csv")
        .display()
        .to_string();

    let lines = count_lines(std::fs::File::open(file.as_str())?)?;
    assert_eq!(6, lines);
    Ok(())
}

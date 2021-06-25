// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[test]
fn test_util_generate_parts() -> Result<()> {
    use common_planners::Part;
    use pretty_assertions::assert_eq;

    use crate::datasources::Common;

    {
        // deal with remainder
        let ps = Common::generate_parts(0, 3, 11);

        assert_eq!(3, ps.len());
        assert_eq!(
            Part {
                name: "11-0-3".into(),
                version: 0
            },
            ps[0]
        );
        assert_eq!(
            Part {
                name: "11-3-6".into(),
                version: 0
            },
            ps[1]
        );
        assert_eq!(
            Part {
                name: "11-6-11".into(),
                version: 0
            },
            ps[2]
        );
    }

    {
        // total is zero
        let ps = Common::generate_parts(0, 3, 0);

        assert_eq!(1, ps.len());
        assert_eq!(
            Part {
                name: "0-0-0".into(),
                version: 0
            },
            ps[0]
        );
    }
    {
        // only one part, total < workers
        let ps = Common::generate_parts(0, 3, 2);

        assert_eq!(1, ps.len());
        assert_eq!(
            Part {
                name: "2-0-2".into(),
                version: 0
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

    use crate::datasources::Common;

    let file = env::current_dir()?
        .join("../../tests/data/sample.csv")
        .display()
        .to_string();

    let lines = Common::count_lines(std::fs::File::open(file.as_str())?)?;
    assert_eq!(6, lines);
    Ok(())
}

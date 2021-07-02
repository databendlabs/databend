// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_version_function() -> anyhow::Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![Test {
        name: "version-function-passed",
        display: "version",
        nullable: false,
        func: VersionFunction::try_create("version")?,
        columns: vec![Series::new(vec![
            "FuseQuery v-0.1.0-3afb26c(1.54.0-nightly-2021-06-09T07:56:09.461981495+00:00)",
        ])
        .into()],
        expect: Series::new(vec![
            "FuseQuery v-0.1.0-3afb26c(1.54.0-nightly-2021-06-09T07:56:09.461981495+00:00)",
        ])
        .into(),
        error: "",
    }];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
        match func.eval(&t.columns, rows) {
            Ok(v) => {
                // Display check.
                let expect_display = t.display.to_string();
                let actual_display = format!("{}", func);
                assert_eq!(expect_display, actual_display);

                assert_eq!(&v, &t.expect);
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }

    Ok(())
}

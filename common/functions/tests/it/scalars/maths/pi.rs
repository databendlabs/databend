// Copyright 2020 Datafuse Labs.
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

use std::f64::consts::PI;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

#[test]
fn test_pi_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        error: &'static str,
        func: Box<dyn Function>,
        expect: DataColumn,
    }
    let tests = vec![Test {
        name: "pi-function-passed",
        display: "pi()",
        func: PiFunction::try_create("pi()")?,
        expect: Series::new(vec![PI]).into(),
        error: "",
    }];

    for t in tests {
        let func = t.func;
        match func.eval(&[], 1) {
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

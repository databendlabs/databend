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
    }
    let tests = vec![Test {
        name: "pi-function-passed",
        display: "rand",
        error: "",
        func: RandomFunction::try_create("rand")?,
    }];

    for t in tests {
        let func = t.func;
        match func.eval(&[], 1) {
            Ok(v) => {
                // Display check.
                let expect_display = t.display.to_string();
                let actual_display = format!("{}", func);
                assert_eq!(expect_display, actual_display);

                v.to_minimal_array()?.f64()?.into_iter().for_each(|rvo| {
                    let rv = rvo.unwrap();
                    assert!((&0.0f64..&1.0f64).contains(&rv));
                });
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }
    Ok(())
}

// Copyright 2020-2022 Jorge C. Leitão
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

use databend_common_arrow::arrow::bitmap::utils::fmt;

struct A<'a>(&'a [u8], usize, usize);

impl<'a> std::fmt::Debug for A<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt(self.0, self.1, self.2, f)
    }
}

#[test]
fn test_debug() -> std::fmt::Result {
    assert_eq!(format!("{:?}", A(&[1], 0, 0)), "[]");
    assert_eq!(format!("{:?}", A(&[0b11000001], 0, 8)), "[0b11000001]");
    assert_eq!(
        format!("{:?}", A(&[0b11000001, 1], 0, 9)),
        "[0b11000001, 0b_______1]"
    );
    assert_eq!(format!("{:?}", A(&[1], 0, 2)), "[0b______01]");
    assert_eq!(format!("{:?}", A(&[1], 1, 2)), "[0b_____00_]");
    assert_eq!(format!("{:?}", A(&[1], 2, 2)), "[0b____00__]");
    assert_eq!(format!("{:?}", A(&[1], 3, 2)), "[0b___00___]");
    assert_eq!(format!("{:?}", A(&[1], 4, 2)), "[0b__00____]");
    assert_eq!(format!("{:?}", A(&[1], 5, 2)), "[0b_00_____]");
    assert_eq!(format!("{:?}", A(&[1], 6, 2)), "[0b00______]");
    assert_eq!(
        format!("{:?}", A(&[0b11000001, 1], 1, 9)),
        "[0b1100000_, 0b______01]"
    );
    // extra bytes are ignored
    assert_eq!(
        format!("{:?}", A(&[0b11000001, 1, 1, 1], 1, 9)),
        "[0b1100000_, 0b______01]"
    );
    assert_eq!(
        format!("{:?}", A(&[0b11000001, 1, 1], 2, 16)),
        "[0b110000__, 0b00000001, 0b______01]"
    );
    Ok(())
}

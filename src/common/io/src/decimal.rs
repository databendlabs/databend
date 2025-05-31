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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;

use ethnum::i256;

pub fn display_decimal_128(num: i128, scale: u8) -> impl Display + Debug {
    struct Decimal {
        num: i128,
        scale: u8,
    }

    impl Display for Decimal {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            if self.scale == 0 {
                write!(f, "{}", self.num)
            } else {
                let pow_scale = 10_i128.pow(self.scale as u32);
                let sign = if self.num.is_negative() { "-" } else { "" };
                let num = self.num.abs();
                write!(
                    f,
                    "{sign}{}.{:0>width$}",
                    num / pow_scale,
                    num % pow_scale,
                    width = self.scale as usize
                )
            }
        }
    }

    impl Debug for Decimal {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            f.write_str(&self.to_string())
        }
    }

    Decimal { num, scale }
}

pub fn display_decimal_256(num: i256, scale: u8) -> impl Display + Debug {
    struct Decimal256 {
        num: i256,
        scale: u8,
    }

    impl Display for Decimal256 {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            if self.scale == 0 {
                write!(f, "{}", self.num)
            } else {
                let pow_scale = i256::from(10).pow(self.scale as u32);
                // -1/10 = 0
                if self.num >= 0 {
                    write!(
                        f,
                        "{}.{:0>width$}",
                        self.num / pow_scale,
                        (self.num % pow_scale).abs(),
                        width = self.scale as usize
                    )
                } else {
                    write!(
                        f,
                        "-{}.{:0>width$}",
                        -self.num / pow_scale,
                        (self.num % pow_scale).abs(),
                        width = self.scale as usize
                    )
                }
            }
        }
    }

    impl Debug for Decimal256 {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result {
            f.write_str(&self.to_string())
        }
    }

    Decimal256 { num, scale }
}

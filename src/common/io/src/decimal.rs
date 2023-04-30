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

use std::fmt::Write;

use ethnum::i256;

pub fn display_decimal_128(num: i128, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = 10_i128.pow(scale as u32);
        if num >= 0 {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        }
    }
    buf
}

pub fn display_decimal_256(num: i256, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = i256::from(10).pow(scale as u32);
        // -1/10 = 0
        if num >= 0 {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        }
    }
    buf
}

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

use std::fmt::Result;
use std::fmt::Write;

use crate::bitmap::Bitmap;

pub fn write_vec<D, F>(
    f: &mut F,
    d: D,
    validity: Option<&Bitmap>,
    len: usize,
    null: &'static str,
    new_lines: bool,
) -> Result
where
    D: Fn(&mut F, usize) -> Result,
    F: Write,
{
    f.write_char('[')?;
    write_list(f, d, validity, len, null, new_lines)?;
    f.write_char(']')?;
    Ok(())
}

fn write_list<D, F>(
    f: &mut F,
    d: D,
    validity: Option<&Bitmap>,
    len: usize,
    null: &'static str,
    new_lines: bool,
) -> Result
where
    D: Fn(&mut F, usize) -> Result,
    F: Write,
{
    for index in 0..len {
        if index != 0 {
            f.write_char(',')?;
            f.write_char(if new_lines { '\n' } else { ' ' })?;
        }
        if let Some(val) = validity {
            if val.get_bit(index) {
                d(f, index)
            } else {
                write!(f, "{null}")
            }
        } else {
            d(f, index)
        }?;
    }
    Ok(())
}

pub fn write_map<D, F>(
    f: &mut F,
    d: D,
    validity: Option<&Bitmap>,
    len: usize,
    null: &'static str,
    new_lines: bool,
) -> Result
where
    D: Fn(&mut F, usize) -> Result,
    F: Write,
{
    f.write_char('{')?;
    write_list(f, d, validity, len, null, new_lines)?;
    f.write_char('}')?;
    Ok(())
}

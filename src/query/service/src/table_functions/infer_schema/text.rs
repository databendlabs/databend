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

use std::sync::LazyLock;

use arrow_schema::DataType;
use regex::RegexSet;

static TEXT_INFER_REGEX_SET: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSet::new([
        r"(?i)^(true)$|^(false)$(?-i)", // BOOLEAN
        r"^-?(\d+)$",                   // INTEGER
        r"^-?((\d*\.\d+|\d+\.\d*)([eE][-+]?\d+)?|\d+([eE][-+]?\d+))$", // DECIMAL
        r"^\d{4}-\d\d-\d\d$",           // DATE32
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:[^\d\.].*)?$", // Timestamp(Second)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,3}(?:[^\d].*)?$", // Timestamp(Millisecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,6}(?:[^\d].*)?$", // Timestamp(Microsecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,9}(?:[^\d].*)?$", // Timestamp(Nanosecond)
    ])
    .expect("failed to build text infer regex set")
});

#[derive(Default, Copy, Clone)]
pub(super) struct TextInferredDataType {
    // 0:Boolean,1:Integer,2:Float64,3:Date32,4:TsS,5:TsMS,6:TsUS,7:TsNS,8:Utf8
    packed: u16,
}

impl TextInferredDataType {
    pub(super) fn get(&self) -> DataType {
        match self.packed {
            0 => DataType::Null,
            1 => DataType::Boolean,
            2 => DataType::Int64,
            4 | 6 => DataType::Float64,
            b if b != 0 && (b & !0b11111000) == 0 => match b.leading_zeros() {
                8 => DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                9 => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                10 => DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                11 => DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                12 => DataType::Date32,
                _ => unreachable!(),
            },
            _ => DataType::Utf8,
        }
    }

    pub(super) fn update(&mut self, field: &[u8]) {
        if field.is_empty() {
            return;
        }

        let Ok(string) = std::str::from_utf8(field) else {
            self.packed |= 1 << 8;
            return;
        };

        self.packed |= if let Some(m) = TEXT_INFER_REGEX_SET.matches(string).into_iter().next() {
            if m == 1 && string.len() >= 19 && string.parse::<i64>().is_err() {
                1 << 8
            } else {
                1 << m
            }
        } else if string == "NaN" || string == "nan" || string == "inf" || string == "-inf" {
            1 << 2
        } else {
            1 << 8
        };
    }
}

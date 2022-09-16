// Copyright 2022 Datafuse Labs.
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

use super::constants::*;

#[derive(Debug)]
pub(crate) struct JEntry {
    pub(crate) type_code: u32,
    pub(crate) length: u32,
}

impl JEntry {
    pub(crate) fn decode_jentry(encoded: u32) -> JEntry {
        let type_code = encoded & JENTRY_TYPE_MASK;
        let length = encoded & JENTRY_OFF_LEN_MASK;
        JEntry { type_code, length }
    }

    pub(crate) fn make_null_jentry() -> JEntry {
        JEntry {
            type_code: NULL_TAG,
            length: 0,
        }
    }

    pub(crate) fn make_true_jentry() -> JEntry {
        JEntry {
            type_code: TRUE_TAG,
            length: 0,
        }
    }

    pub(crate) fn make_false_jentry() -> JEntry {
        JEntry {
            type_code: FALSE_TAG,
            length: 0,
        }
    }

    pub(crate) fn make_string_jentry(length: usize) -> JEntry {
        JEntry {
            type_code: STRING_TAG,
            length: length as u32,
        }
    }

    pub(crate) fn make_number_jentry(length: usize) -> JEntry {
        JEntry {
            type_code: NUMBER_TAG,
            length: length as u32,
        }
    }

    pub(crate) fn make_container_jentry(length: usize) -> JEntry {
        JEntry {
            type_code: CONTAINER_TAG,
            length: length as u32,
        }
    }

    pub(crate) fn encoded(&self) -> u32 {
        self.type_code | self.length
    }
}

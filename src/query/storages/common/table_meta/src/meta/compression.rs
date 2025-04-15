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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Copy, Clone, Debug)]
pub enum Compression {
    // Lz4 will be deprecated.
    Lz4,
    Lz4Raw,
    Snappy,
    Zstd,
    Gzip,
    // New: Added by bohu.
    None,
}

impl Compression {
    pub fn legacy() -> Self {
        Compression::Lz4
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Compression::Lz4 => 0,
            Compression::Lz4Raw => 1,
            Compression::Snappy => 2,
            Compression::Zstd => 3,
            Compression::Gzip => 4,
            Compression::None => 5,
        }
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Compression::Lz4,
            1 => Compression::Lz4Raw,
            2 => Compression::Snappy,
            3 => Compression::Zstd,
            4 => Compression::Gzip,
            5 => Compression::None,
            _ => unreachable!(),
        }
    }
}

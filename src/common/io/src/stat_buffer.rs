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

use std::fmt::Debug;

pub trait StatBuffer {
    type Buffer: AsMut<[u8]> + AsRef<[u8]> + Copy + Sync + Debug;
    fn buffer() -> Self::Buffer;
}

impl StatBuffer for u8 {
    type Buffer = [Self; 1];

    fn buffer() -> Self::Buffer {
        [0; 1]
    }
}

impl StatBuffer for u16 {
    type Buffer = [u8; 2];

    fn buffer() -> Self::Buffer {
        [0; 2]
    }
}

impl StatBuffer for u32 {
    type Buffer = [u8; 4];

    fn buffer() -> Self::Buffer {
        [0; 4]
    }
}

impl StatBuffer for u64 {
    type Buffer = [u8; 8];

    fn buffer() -> Self::Buffer {
        [0; 8]
    }
}

impl StatBuffer for i8 {
    type Buffer = [u8; 1];

    fn buffer() -> Self::Buffer {
        [0; 1]
    }
}

impl StatBuffer for i16 {
    type Buffer = [u8; 2];

    fn buffer() -> Self::Buffer {
        [0; 2]
    }
}

impl StatBuffer for i32 {
    type Buffer = [u8; 4];

    fn buffer() -> Self::Buffer {
        [0; 4]
    }
}

impl StatBuffer for i64 {
    type Buffer = [u8; 8];

    fn buffer() -> Self::Buffer {
        [0; 8]
    }
}

impl StatBuffer for f32 {
    type Buffer = [u8; 4];

    fn buffer() -> Self::Buffer {
        [0; 4]
    }
}

impl StatBuffer for f64 {
    type Buffer = [u8; 8];

    fn buffer() -> Self::Buffer {
        [0; 8]
    }
}

impl StatBuffer for bool {
    type Buffer = [u8; 1];

    fn buffer() -> Self::Buffer {
        [0; 1]
    }
}

impl StatBuffer for char {
    type Buffer = [u8; 4];

    fn buffer() -> Self::Buffer {
        [0; 4]
    }
}

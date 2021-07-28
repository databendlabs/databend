// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub trait StatBuffer {
    type Buffer: AsMut<[u8]> + AsRef<[u8]> + Copy + Sync;
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

// Copyright 2020 Datafuse Labs.
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
// limitations under the License

pub trait Unmarshal<T: Copy> {
    fn unmarshal(scratch: &[u8]) -> T;
}

impl Unmarshal<u8> for u8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0]
    }
}

impl Unmarshal<u16> for u16 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0]) | Self::from(scratch[1]) << 8
    }
}

impl Unmarshal<u32> for u32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0])
            | Self::from(scratch[1]) << 8
            | Self::from(scratch[2]) << 16
            | Self::from(scratch[3]) << 24
    }
}

impl Unmarshal<u64> for u64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0])
            | Self::from(scratch[1]) << 8
            | Self::from(scratch[2]) << 16
            | Self::from(scratch[3]) << 24
            | Self::from(scratch[4]) << 32
            | Self::from(scratch[5]) << 40
            | Self::from(scratch[6]) << 48
            | Self::from(scratch[7]) << 56
    }
}

impl Unmarshal<i8> for i8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] as Self
    }
}

impl Unmarshal<i16> for i16 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0]) | Self::from(scratch[1]) << 8
    }
}

impl Unmarshal<i32> for i32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0])
            | Self::from(scratch[1]) << 8
            | Self::from(scratch[2]) << 16
            | Self::from(scratch[3]) << 24
    }
}

impl Unmarshal<i64> for i64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        Self::from(scratch[0])
            | Self::from(scratch[1]) << 8
            | Self::from(scratch[2]) << 16
            | Self::from(scratch[3]) << 24
            | Self::from(scratch[4]) << 32
            | Self::from(scratch[5]) << 40
            | Self::from(scratch[6]) << 48
            | Self::from(scratch[7]) << 56
    }
}

impl Unmarshal<f32> for f32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u32::from(scratch[0])
            | u32::from(scratch[1]) << 8
            | u32::from(scratch[2]) << 16
            | u32::from(scratch[3]) << 24;
        Self::from_bits(bits)
    }
}

impl Unmarshal<f64> for f64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u64::from(scratch[0])
            | u64::from(scratch[1]) << 8
            | u64::from(scratch[2]) << 16
            | u64::from(scratch[3]) << 24
            | u64::from(scratch[4]) << 32
            | u64::from(scratch[5]) << 40
            | u64::from(scratch[6]) << 48
            | u64::from(scratch[7]) << 56;
        Self::from_bits(bits)
    }
}

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] != 0
    }
}

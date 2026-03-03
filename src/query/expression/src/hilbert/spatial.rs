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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::types::F64;

pub fn hilbert_index_from_point(x: f64, y: f64) -> Result<u64> {
    let x_f32 = x as f32;
    let y_f32 = y as f32;
    if !x_f32.is_finite() || !y_f32.is_finite() {
        return Err(ErrorCode::GeometryError(
            "ST_HILBERT coordinates must be finite".to_string(),
        ));
    }
    let x_u32 = hilbert_f32_to_u32(x_f32);
    let y_u32 = hilbert_f32_to_u32(y_f32);
    Ok(u64::from(hilbert_encode(16, x_u32, y_u32)))
}

pub fn hilbert_index_from_bounds_slice(x: f64, y: f64, bounds: &[F64]) -> Result<u64> {
    if bounds.len() != 4 {
        return Err(ErrorCode::GeometryError(
            "ST_HILBERT bounds must have 4 elements".to_string(),
        ));
    }
    let (xmin, ymin, xmax, ymax) = (bounds[0].0, bounds[1].0, bounds[2].0, bounds[3].0);

    hilbert_index_from_bounds(x, y, xmin, ymin, xmax, ymax)
}

pub fn hilbert_index_from_bounds(
    x: f64,
    y: f64,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> Result<u64> {
    let (x_u32, y_u32) = hilbert_normalize_bounds(x, y, xmin, ymin, xmax, ymax)?;
    Ok(u64::from(hilbert_encode(16, x_u32, y_u32)))
}

fn hilbert_f32_to_u32(value: f32) -> u32 {
    if value.is_nan() {
        return u32::MAX;
    }
    let bits = value.to_bits();
    if bits & 0x8000_0000 != 0 {
        bits ^ 0xFFFF_FFFF
    } else {
        bits | 0x8000_0000
    }
}

fn hilbert_encode(n: u32, mut x: u32, mut y: u32) -> u32 {
    x <<= 16 - n;
    y <<= 16 - n;

    let mut a = x ^ y;
    let mut b = 0xFFFF ^ a;
    let mut c = 0xFFFF ^ (x | y);
    let mut d = x & (y ^ 0xFFFF);
    let mut a0 = a | (b >> 1);
    let mut b0 = (a >> 1) ^ a;
    let mut c0 = ((c >> 1) ^ (b & (d >> 1))) ^ c;
    let mut d0 = ((a & (c >> 1)) ^ (d >> 1)) ^ d;

    a = a0;
    b = b0;
    c = c0;
    d = d0;
    a0 = (a & (a >> 2)) ^ (b & (b >> 2));
    b0 = (a & (b >> 2)) ^ (b & ((a ^ b) >> 2));
    c0 ^= (a & (c >> 2)) ^ (b & (d >> 2));
    d0 ^= (b & (c >> 2)) ^ ((a ^ b) & (d >> 2));

    a = a0;
    b = b0;
    c = c0;
    d = d0;
    a0 = (a & (a >> 4)) ^ (b & (b >> 4));
    b0 = (a & (b >> 4)) ^ (b & ((a ^ b) >> 4));
    c0 ^= (a & (c >> 4)) ^ (b & (d >> 4));
    d0 ^= (b & (c >> 4)) ^ ((a ^ b) & (d >> 4));

    a = a0;
    b = b0;
    c = c0;
    d = d0;
    c0 ^= (a & (c >> 8)) ^ (b & (d >> 8));
    d0 ^= (b & (c >> 8)) ^ ((a ^ b) & (d >> 8));

    a = c0 ^ (c0 >> 1);
    b = d0 ^ (d0 >> 1);

    let i0 = x ^ y;
    let i1 = b | (0xFFFF ^ (i0 | a));

    ((hilbert_interleave(i1) << 1) | hilbert_interleave(i0)) >> (32 - 2 * n)
}

fn hilbert_normalize_bounds(
    x: f64,
    y: f64,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
) -> Result<(u32, u32)> {
    if !x.is_finite()
        || !y.is_finite()
        || !xmin.is_finite()
        || !ymin.is_finite()
        || !xmax.is_finite()
        || !ymax.is_finite()
    {
        return Err(ErrorCode::GeometryError(
            "ST_HILBERT bounds must be finite".to_string(),
        ));
    }
    let span_x = xmax - xmin;
    let span_y = ymax - ymin;
    if span_x <= 0.0 || span_y <= 0.0 {
        return Err(ErrorCode::GeometryError(
            "ST_HILBERT bounds must be increasing".to_string(),
        ));
    }
    let max_hilbert = u16::MAX as f64;
    let hilbert_width = max_hilbert / span_x;
    let hilbert_height = max_hilbert / span_y;
    // Clamp to the 16-bit Hilbert grid for out-of-bounds coordinates.
    let hilbert_x = ((x - xmin) * hilbert_width).clamp(0.0, max_hilbert) as u32;
    let hilbert_y = ((y - ymin) * hilbert_height).clamp(0.0, max_hilbert) as u32;
    Ok((hilbert_x, hilbert_y))
}

fn hilbert_interleave(mut x: u32) -> u32 {
    x = (x | (x << 8)) & 0x00FF_00FF;
    x = (x | (x << 4)) & 0x0F0F_0F0F;
    x = (x | (x << 2)) & 0x3333_3333;
    x = (x | (x << 1)) & 0x5555_5555;
    x
}

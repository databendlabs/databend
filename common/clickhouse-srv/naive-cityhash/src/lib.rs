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

#![no_std]

use core::mem::size_of;
use core::ptr::read_unaligned;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct U128 {
    pub lo: u64,
    pub hi: u64,
}

impl U128 {
    #[inline]
    pub const fn new(lo: u64, hi: u64) -> Self {
        Self { lo, hi }
    }

    #[inline]
    pub const fn lo(&self) -> u64 {
        self.lo
    }

    #[inline]
    pub const fn hi(&self) -> u64 {
        self.hi
    }
}

impl From<u128> for U128 {
    fn from(source: u128) -> Self {
        Self {
            lo: source as u64,
            hi: (source >> 64) as u64,
        }
    }
}

impl From<U128> for u128 {
    fn from(val: U128) -> Self {
        (val.lo as u128) | ((val.hi as u128) << 64)
    }
}

const K0: u64 = 0xc3a5c85c97cb3127u64;
const K1: u64 = 0xb492b66fbe98f273u64;
const K2: u64 = 0x9ae16a3b2f90404fu64;
const K3: u64 = 0xc949d7c7509e6557u64;

#[cfg(target_endian = "little")]
#[inline]
pub fn fetch64(data: &[u8]) -> u64 {
    debug_assert!(data.len() >= size_of::<u64>());
    let ptr = data.as_ptr() as *const u64;
    unsafe { read_unaligned(ptr) }
}

#[cfg(target_endian = "little")]
#[inline]
fn fetch32(data: &[u8]) -> u32 {
    debug_assert!(data.len() >= size_of::<u32>());
    let ptr = data.as_ptr() as *const u32;
    unsafe { read_unaligned(ptr) }
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn fetch64(data: &[u8]) -> u64 {
    debug_assert!(data.len() >= mem::size_of::<u64>());
    let ptr = data.as_ptr() as *const u64;
    let data = unsafe { read_unaligned(ptr) };
    data.swap_bytes()
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn fetch32(data: &[u8]) -> u32 {
    debug_assert!(data.len() >= size_of::<u32>());
    let ptr = data.as_ptr() as *const u32;
    let data = unsafe { read_unaligned(ptr) };
    data.swap_bytes()
}

// rotate, but `shift` must not be eq 0
#[inline(always)]
fn rotate_least(val: u64, shift: u64) -> u64 {
    (val >> shift) | (val << (64 - shift))
}

#[inline(always)]
fn shift_mix(val: u64) -> u64 {
    val ^ (val >> 47)
}

fn hash_len16(u: u64, v: u64) -> u64 {
    hash128_to_64(u, v)
}

#[inline(always)]
fn hash128_to_64(l: u64, h: u64) -> u64 {
    const K_MUL: u64 = 0x9ddfea08eb382d69u64;
    let mut a = (h ^ l).wrapping_mul(K_MUL);
    a ^= a >> 47;
    let mut b = (h ^ a).wrapping_mul(K_MUL);
    b ^= b >> 47;
    b.wrapping_mul(K_MUL)
}

fn hash_len0to16(data: &[u8]) -> u64 {
    if data.len() > 8 {
        let a = fetch64(data);
        let b = fetch64(&data[data.len() - 8..]);
        b ^ hash_len16(
            a,
            rotate_least(b.wrapping_add(data.len() as u64), data.len() as u64),
        )
    } else if data.len() >= 4 {
        let a = fetch32(data) as u64;

        hash_len16(
            (a << 3).wrapping_add(data.len() as u64),
            fetch32(&data[data.len() - 4..]) as u64,
        )
    } else if !data.is_empty() {
        let a: u8 = data[0];
        let b: u8 = data[data.len() >> 1];
        let c: u8 = data[data.len() - 1];
        let y = (a as u64).wrapping_add((b as u64) << 8);
        let z = (data.len() as u64).wrapping_add((c as u64) << 2);

        shift_mix(y.wrapping_mul(K2) ^ z.wrapping_mul(K3)).wrapping_mul(K2)
    } else {
        K2
    }
}

fn weak_hash_len32_with_seeds(data: &[u8], a: u64, b: u64) -> (u64, u64) {
    _weak_hash_len32_with_seeds(
        fetch64(data),
        fetch64(&data[8..]),
        fetch64(&data[16..]),
        fetch64(&data[24..]),
        a,
        b,
    )
}

#[inline(always)]
fn _weak_hash_len32_with_seeds(
    w: u64,
    x: u64,
    y: u64,
    z: u64,
    mut a: u64,
    mut b: u64,
) -> (u64, u64) {
    a = a.wrapping_add(w);
    b = rotate_least(b.wrapping_add(a).wrapping_add(z), 21);
    let c = a;
    a = a.wrapping_add(x).wrapping_add(y);
    b = b.wrapping_add(rotate_least(a, 44));
    (a.wrapping_add(z), b.wrapping_add(c))
}

fn city_murmur(mut data: &[u8], seed: U128) -> U128 {
    let mut a = seed.lo;
    let mut b = seed.hi;
    let mut c: u64;
    let mut d: u64;

    if data.len() <= 16 {
        a = shift_mix(a.wrapping_mul(K1)).wrapping_mul(K1);
        c = b.wrapping_mul(K1).wrapping_add(hash_len0to16(data));
        d = if data.len() >= 8 { fetch64(data) } else { c };
        d = shift_mix(a.wrapping_add(d));
    } else {
        c = hash_len16(fetch64(&data[data.len() - 8..]).wrapping_add(K1), a);
        d = hash_len16(
            b.wrapping_add(data.len() as u64),
            c.wrapping_add(fetch64(&data[data.len() - 16..])),
        );
        a = a.wrapping_add(d);
        loop {
            a ^= shift_mix(fetch64(data).wrapping_mul(K1)).wrapping_mul(K1);
            a = a.wrapping_mul(K1);
            b ^= a;
            c ^= shift_mix(fetch64(&data[8..]).wrapping_mul(K1)).wrapping_mul(K1);
            c = c.wrapping_mul(K1);
            d ^= c;
            data = &data[16..];
            if data.len() <= 16 {
                break;
            }
        }
    }

    a = hash_len16(a, c);
    b = hash_len16(d, b);
    U128::new(a ^ b, hash_len16(b, a))
}

fn cityhash128_with_seed(mut data: &[u8], seed: U128) -> U128 {
    if data.len() < 128 {
        return city_murmur(data, seed);
    }

    let mut x = seed.lo;
    let mut y = seed.hi;
    let mut z = K1.wrapping_mul(data.len() as u64);
    let t: u64 = K1
        .wrapping_mul(rotate_least(y ^ K1, 49))
        .wrapping_add(fetch64(data));
    let mut v = (
        t,
        K1.wrapping_mul(rotate_least(t, 42))
            .wrapping_add(fetch64(&data[8..])),
    );
    let mut w = (
        K1.wrapping_mul(rotate_least(y.wrapping_add(z), 35))
            .wrapping_add(x),
        K1.wrapping_mul(rotate_least(x.wrapping_add(fetch64(&data[88..])), 53)),
    );

    loop {
        x = K1.wrapping_mul(rotate_least(
            x.wrapping_add(y)
                .wrapping_add(v.0)
                .wrapping_add(fetch64(&data[16..])),
            37,
        ));
        y = K1.wrapping_mul(rotate_least(
            y.wrapping_add(v.1).wrapping_add(fetch64(&data[48..])),
            42,
        ));
        x ^= w.1;
        y ^= v.0;
        z = rotate_least(z ^ w.0, 33);
        v = weak_hash_len32_with_seeds(data, K1.wrapping_mul(v.1), x.wrapping_add(w.0));
        w = weak_hash_len32_with_seeds(&data[32..], z.wrapping_add(w.1), y);
        core::mem::swap(&mut z, &mut x);

        data = &data[64..];
        x = K1.wrapping_mul(rotate_least(
            x.wrapping_add(y)
                .wrapping_add(v.0)
                .wrapping_add(fetch64(&data[16..])),
            37,
        ));
        y = K1.wrapping_mul(rotate_least(
            y.wrapping_add(v.1).wrapping_add(fetch64(&data[48..])),
            42,
        ));
        x ^= w.1;
        y ^= v.0;
        z = rotate_least(z ^ w.0, 33);
        v = weak_hash_len32_with_seeds(data, K1.wrapping_mul(v.1), x.wrapping_add(w.0));
        w = weak_hash_len32_with_seeds(&data[32..], z.wrapping_add(w.1), y);
        core::mem::swap(&mut z, &mut x);
        if data.len() < (128 + 64) {
            break;
        }
        data = &data[64..];
    }

    y = y.wrapping_add(K0.wrapping_mul(rotate_least(w.0, 37)).wrapping_add(z));
    x = x.wrapping_add(K0.wrapping_mul(rotate_least(v.0.wrapping_add(z), 49)));

    while data.len() > 64 {
        y = K0
            .wrapping_mul(rotate_least(y.wrapping_sub(x), 42))
            .wrapping_add(v.1);
        w.0 = w.0.wrapping_add(fetch64(&data[data.len() - 16..]));
        x = K0.wrapping_mul(rotate_least(x, 49)).wrapping_add(w.0);
        w.0 = w.0.wrapping_add(v.0);
        v = weak_hash_len32_with_seeds(&data[data.len() - 32..], v.0, v.1);
        data = &data[0..data.len() - 32];
    }

    x = hash_len16(x, v.0);
    y = hash_len16(y, w.0);

    U128 {
        lo: hash_len16(x.wrapping_add(v.1), w.1).wrapping_add(y),
        hi: hash_len16(x.wrapping_add(w.1), y.wrapping_add(v.1)),
    }
}

#[inline]
pub fn cityhash128(data: &[u8]) -> U128 {
    if data.len() >= 16 {
        cityhash128_with_seed(&data[16..], U128 {
            lo: fetch64(data) ^ K3,
            hi: fetch64(&data[8..]),
        })
    } else if data.len() >= 8 {
        cityhash128_with_seed(b"", U128 {
            lo: fetch64(data) ^ (K0.wrapping_mul(data.len() as u64)),
            hi: fetch64(&data[data.len() - 8..]) ^ K1,
        })
    } else {
        cityhash128_with_seed(data, U128 { lo: K0, hi: K1 })
    }
}

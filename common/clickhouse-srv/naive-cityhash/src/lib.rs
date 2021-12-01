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

use core::num::Wrapping;

type W64 = Wrapping<u64>;
type W32 = Wrapping<u32>;

const fn w64(v: u64) -> W64 {
    Wrapping(v)
}

const fn w32(v: u32) -> W32 {
    Wrapping(v)
}

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

    const fn from_w64(lo: W64, hi: W64) -> Self {
        Self { lo: lo.0, hi: hi.0 }
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

const K0: W64 = w64(0xc3a5c85c97cb3127u64);
const K1: W64 = w64(0xb492b66fbe98f273u64);
const K2: W64 = w64(0x9ae16a3b2f90404fu64);
const K3: W64 = w64(0xc949d7c7509e6557u64);

#[inline]
fn fetch64(s: *const u8) -> W64 {
    w64(unsafe { (s as *const u64).read_unaligned().to_le() })
}

#[inline]
fn fetch32(s: *const u8) -> W32 {
    w32(unsafe { (s as *const u32).read_unaligned().to_le() })
}

#[inline]
fn rotate(v: W64, n: u32) -> W64 {
    debug_assert!(n > 0);
    w64(v.0.rotate_right(n))
}

fn hash_len16(u: W64, v: W64) -> W64 {
    hash128_to_64(u, v)
}

#[inline]
fn hash128_to_64(l: W64, h: W64) -> W64 {
    const K_MUL: W64 = w64(0x9ddfea08eb382d69u64);
    let mut a = (h ^ l) * K_MUL;
    a ^= a >> 47;
    let mut b = (h ^ a) * K_MUL;
    b ^= b >> 47;
    b * K_MUL
}

fn hash_len0to16(data: &[u8]) -> W64 {
    let len = data.len();
    let s = data.as_ptr();

    if data.len() > 8 {
        unsafe {
            let a = fetch64(s);
            let b = fetch64(s.add(len).sub(8));
            b ^ hash_len16(a, rotate(b + w64(len as u64), len as u32))
        }
    } else if len >= 4 {
        unsafe {
            let a = fetch32(s).0 as u64;

            hash_len16(
                w64((len as u64) + (a << 3)),
                w64(fetch32(s.add(len).sub(4)).0.into()),
            )
        }
    } else if len > 0 {
        let a: u8 = data[0];
        let b: u8 = data[len >> 1];
        let c: u8 = data[len - 1];
        let y = w64(a as u64) + w64((b as u64) << 8);
        let z = w64(((len as u32) + ((c as u32) << 2)) as u64);

        shift_mix((y * K2) ^ (z * K3)) * K2
    } else {
        K2
    }
}

unsafe fn weak_hash_len32_with_seeds(s: *const u8, a: W64, b: W64) -> (W64, W64) {
    weak_hash_len32_with_seeds_(
        fetch64(s),
        fetch64(s.add(8)),
        fetch64(s.add(16)),
        fetch64(s.add(24)),
        a,
        b,
    )
}

fn weak_hash_len32_with_seeds_(
    w: W64,
    x: W64,
    y: W64,
    z: W64,
    mut a: W64,
    mut b: W64,
) -> (W64, W64) {
    a += w;
    b = rotate(b + a + z, 21);
    let c = a;
    a += x + y;
    b += rotate(a, 44);
    (a + z, b + c)
}

fn shift_mix(val: W64) -> W64 {
    val ^ (val >> 47)
}

fn city_murmur(data: &[u8], seed: U128) -> U128 {
    let mut s = data.as_ptr();
    let len = data.len();

    let mut a = w64(seed.lo);
    let mut b = w64(seed.hi);
    let mut c: W64;
    let mut d: W64;
    let mut l = (len as isize) - 16;

    if l <= 0 {
        a = shift_mix(a * K1) * K1;
        c = b * K1 + hash_len0to16(data);
        d = shift_mix(a + (if len >= 8 { fetch64(s) } else { c }));
    } else {
        unsafe {
            c = hash_len16(fetch64(s.add(len).sub(8)) + K1, a);
            d = hash_len16(b + w64(len as u64), c + fetch64(s.add(len).sub(16)));
            a += d;
            loop {
                a ^= shift_mix(fetch64(s) * K1) * K1;
                a *= K1;
                b ^= a;
                c ^= shift_mix(fetch64(s.add(8)) * K1) * K1;
                c *= K1;
                d ^= c;
                s = s.add(16);
                l -= 16;
                if l <= 0 {
                    break;
                }
            }
        }
    }
    a = hash_len16(a, c);
    b = hash_len16(d, b);
    U128::from_w64(a ^ b, hash_len16(b, a))
}

fn cityhash128_with_seed(data: &[u8], seed: U128) -> U128 {
    let mut s = data.as_ptr();
    let mut len = data.len();

    unsafe {
        if len < 128 {
            return city_murmur(data, seed);
        }

        let mut x = w64(seed.lo);
        let mut y = w64(seed.hi);
        let mut z = w64(len as u64) * K1;
        let mut v = (w64(0), w64(0));
        v.0 = rotate(y ^ K1, 49) * K1 + fetch64(s);
        v.1 = rotate(v.0, 42) * K1 + fetch64(s.add(8));
        let mut w = (
            rotate(y + z, 35) * K1 + x,
            rotate(x + fetch64(s.add(88)), 53) * K1,
        );

        loop {
            x = rotate(x + y + v.0 + fetch64(s.add(16)), 37) * K1;
            y = rotate(y + v.1 + fetch64(s.add(48)), 42) * K1;
            x ^= w.1;
            y ^= v.0;
            z = rotate(z ^ w.0, 33);
            v = weak_hash_len32_with_seeds(s, v.1 * K1, x + w.0);
            w = weak_hash_len32_with_seeds(s.add(32), z + w.1, y);
            core::mem::swap(&mut z, &mut x);
            s = s.add(64);
            x = rotate(x + y + v.0 + fetch64(s.add(16)), 37) * K1;
            y = rotate(y + v.1 + fetch64(s.add(48)), 42) * K1;
            x ^= w.1;
            y ^= v.0;
            z = rotate(z ^ w.0, 33);
            v = weak_hash_len32_with_seeds(s, v.1 * K1, x + w.0);
            w = weak_hash_len32_with_seeds(s.add(32), z + w.1, y);
            core::mem::swap(&mut z, &mut x);
            s = s.add(64);
            len -= 128;

            if len < 128 {
                break;
            }
        }

        y += rotate(w.0, 37) * K0 + z;
        x += rotate(v.0 + z, 49) * K0;

        let mut tail_done: usize = 0;
        while tail_done < len {
            tail_done += 32;
            y = rotate(y - x, 42) * K0 + v.1;
            w.0 += fetch64(s.add(len).sub(tail_done).add(16));
            x = rotate(x, 49) * K0 + w.0;
            w.0 += v.0;
            v = weak_hash_len32_with_seeds(s.add(len).sub(tail_done), v.0, v.1);
        }

        x = hash_len16(x, v.0);
        y = hash_len16(y, w.0);

        U128::from_w64(hash_len16(x + v.1, w.1) + y, hash_len16(x + w.1, y + v.1))
    }
}

#[inline]
pub fn cityhash128(data: &[u8]) -> U128 {
    let s = data.as_ptr();
    let len = data.len();
    unsafe {
        if len >= 16 {
            cityhash128_with_seed(
                &data[16..],
                U128::from_w64(fetch64(s) ^ K3, fetch64(s.add(8))),
            )
        } else if data.len() >= 8 {
            cityhash128_with_seed(
                b"",
                U128::from_w64(
                    fetch64(s) ^ (w64(len as u64) * K0),
                    fetch64(s.add(len).sub(8)) ^ K1,
                ),
            )
        } else {
            cityhash128_with_seed(data, U128::from_w64(K0, K1))
        }
    }
}

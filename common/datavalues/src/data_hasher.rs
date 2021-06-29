// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub trait DataHasher {
    fn hash_bool(v: &bool) -> u64;

    fn hash_i8(v: &i8) -> u64;
    fn hash_i16(v: &i16) -> u64;
    fn hash_i32(v: &i32) -> u64;
    fn hash_i64(v: &i64) -> u64;

    fn hash_u8(v: &u8) -> u64;
    fn hash_u16(v: &u16) -> u64;
    fn hash_u32(v: &u32) -> u64;
    fn hash_u64(v: &u64) -> u64;

    fn hash_f32(v: &f32) -> u64;
    fn hash_f64(v: &f64) -> u64;

    fn hash_bytes(bytes: &[u8]) -> u64;
}

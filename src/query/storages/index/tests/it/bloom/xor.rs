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

use common_storages_index::bloom::Bloom;
use rand::prelude::random;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use xorfilter::BuildHasherDefault;
use xorfilter::Xor8;

#[test]
fn test_xor_bitmap_u64() {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = 8 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<u64> = (0..numbers).map(|_| rng.gen::<u64>()).collect();

    let mut filter = Xor8::<BuildHasherDefault>::new();
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build().unwrap();

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    {
        let val = <Xor8 as Bloom>::to_bytes(&filter).unwrap();
        let (_, n) = <Xor8<BuildHasherDefault> as Bloom>::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());

        // u64 bitmap enc:1230069, raw:8000000, ratio:0.15375863
        println!(
            "u64 bitmap enc:{}, raw:{}, ratio:{}",
            val.len(),
            size,
            val.len() as f32 / size as f32
        );
    }
}

#[test]
fn test_xor_bitmap_bool() {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<bool> = (0..numbers).map(|_| rng.gen::<u64>() % 2 == 0).collect();

    let mut filter = Xor8::<BuildHasherDefault>::new();
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build().unwrap();

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    {
        let val = <Xor8 as Bloom>::to_bytes(&filter).unwrap();
        let (_, n) = <Xor8<BuildHasherDefault> as Bloom>::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());

        // bool bitmap enc:61, raw:1000000, ratio:0.000061
        println!(
            "bool bitmap enc:{}, raw:{}, ratio:{}",
            val.len(),
            size,
            val.len() as f32 / size as f32
        );
    }
}

#[test]
fn test_xor_bitmap_string() {
    let seed: u64 = random();
    let numbers = 100_000;

    let len = 30;
    let size = 30 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";

    let keys: Vec<String> = (0..numbers)
        .map(|_| {
            (0..len)
                .map(|_| {
                    let idx = rng.gen_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect()
        })
        .collect();

    let mut filter = Xor8::<BuildHasherDefault>::new();
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build().unwrap();

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    {
        let val = <Xor8 as Bloom>::to_bytes(&filter).unwrap();
        let (_, n) = <Xor8<BuildHasherDefault> as Bloom>::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());

        // string enc:123067, raw:3000000, ratio:0.041022334
        println!(
            "string enc:{}, raw:{}, ratio:{}",
            val.len(),
            size,
            val.len() as f32 / size as f32
        );
    }
}

#[test]
fn test_xor_bitmap_duplicate_string() {
    let numbers = 100_000;

    let key = "123456789012345678901234567890";
    let len = key.len();
    let size = len * numbers;

    let keys: Vec<String> = (0..numbers).map(|_| key.to_string()).collect();

    let mut filter = Xor8::<BuildHasherDefault>::new();
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build().unwrap();

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    {
        let val = <Xor8 as Bloom>::to_bytes(&filter).unwrap();
        let (_, n) = <Xor8<BuildHasherDefault> as Bloom>::from_bytes(&val).unwrap();
        assert_eq!(n, val.len(), "{} {}", n, val.len());

        // string enc:61, raw:3000000, ratio:0.000020333333
        println!(
            "string enc:{}, raw:{}, ratio:{}",
            val.len(),
            size,
            val.len() as f32 / size as f32
        );
    }
}

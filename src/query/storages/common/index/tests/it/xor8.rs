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

use databend_common_expression::ScalarRef;
use databend_common_expression::types::number::NumberScalar;
use databend_storages_common_index::filters::BinaryFuse32Builder;
use databend_storages_common_index::filters::BinaryFuse32Filter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterBuilder;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_index::filters::Xor8Builder;
use databend_storages_common_index::filters::Xor8Filter;
use rand::Rng;
use rand::SeedableRng;
use rand::prelude::random;
use rand::rngs::StdRng;

#[test]
fn test_xor_bitmap_u64() -> anyhow::Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = 8 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<u64> = (0..numbers).map(|_| rng.r#gen::<u64>()).collect();

    let mut builder = Xor8Builder::create();
    builder.add_keys(&keys);
    let filter = builder.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 1230069);

    // u64 bitmap enc:1230069, raw:8000000, ratio:0.15375863
    println!(
        "u64 bitmap enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_bool() -> anyhow::Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<bool> = (0..numbers).map(|_| rng.r#gen::<u64>() % 2 == 0).collect();

    let mut builder = Xor8Builder::create();
    builder.add_keys(&keys);
    let filter = builder.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 61);

    // bool bitmap enc:61, raw:1000000, ratio:0.000061
    println!(
        "bool bitmap enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_string() -> anyhow::Result<()> {
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

    let mut builder = Xor8Builder::create();
    builder.add_keys(&keys);
    let filter = builder.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 123067);

    // string enc:123067, raw:3000000, ratio:0.041022334
    println!(
        "string enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_duplicate_string() -> anyhow::Result<()> {
    let numbers = 100_000;

    let key = "123456789012345678901234567890";
    let len = key.len();
    let size = len * numbers;

    let keys: Vec<String> = (0..numbers).map(|_| key.to_string()).collect();

    let mut builder = Xor8Builder::create();
    builder.add_keys(&keys);
    let filter = builder.build()?;

    assert!(filter.contains(&keys[0]), "key {} not present", key);

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 61);

    // string enc:61, raw:3000000, ratio:0.000020333333
    println!(
        "string enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_data_block() -> anyhow::Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;
    let size = 8 * numbers;

    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<i64> = (0..numbers).map(|_| rng.r#gen::<i64>()).collect();

    let mut builder = Xor8Builder::create();
    keys.iter()
        .for_each(|key| builder.add_key(&ScalarRef::Number(NumberScalar::Int64(*key))));
    let filter = builder.build()?;

    for key in keys {
        assert!(
            filter.contains(&ScalarRef::Number(NumberScalar::Int64(key))),
            "key {} is not present",
            key
        );
    }

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // data block enc:1230069, raw:8000000, ratio:0.15375863
    // Actually, it not related to datablock, it related to the type of the column
    // Here it same as u64.
    println!(
        "data block(i64) enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_from_digests() -> anyhow::Result<()> {
    let numbers = 1_000_000;

    let size = 8 * numbers;
    let digests: Vec<u64> = (0..numbers).collect();
    let mut builder = Xor8Builder::create();
    builder.add_digests(&digests);
    let filter = builder.build()?;
    for digest in digests.iter() {
        assert!(
            filter.contains_digest(*digest),
            "digests {} not present",
            digest
        );
    }

    let val = filter.to_bytes()?;
    let (_, n) = Xor8Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 1230069);

    // u64 bitmap enc:1230069, raw:8000000, ratio:0.15375863
    println!(
        "u64 bitmap enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_binary_fuse32_from_duplicate_digests() -> anyhow::Result<()> {
    let digests = vec![7_u64, 7, 13, 13, 29];

    let mut builder = BinaryFuse32Builder::create();
    builder.add_digests(&digests);
    let filter = builder.build()?;

    assert_eq!(filter.len(), Some(3));
    for digest in [7_u64, 13, 29] {
        assert!(
            filter.contains_digest(digest),
            "digest {} not present",
            digest
        );
    }

    let val = filter.to_bytes()?;
    let (decoded, n) = BinaryFuse32Filter::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());
    assert_eq!(decoded.len(), Some(3));
    assert!(decoded.contains_digest(13));

    Ok(())
}

#[test]
fn test_filter_impl_dispatch_binary_fuse32() -> anyhow::Result<()> {
    let digests = vec![3_u64, 5, 8];
    let mut builder = BinaryFuse32Builder::create();
    builder.add_digests(&digests);
    let filter = builder.build()?;

    let bytes = FilterImpl::BinaryFuse32(filter.clone()).to_bytes()?;
    assert_eq!(bytes[0], b'f');

    let (decoded, n) = FilterImpl::from_bytes(&bytes)?;
    assert_eq!(n, bytes.len());
    match decoded {
        FilterImpl::BinaryFuse32(filter) => {
            assert_eq!(filter.len(), Some(3));
            assert!(filter.contains_digest(5));
        }
        _ => panic!("expected binary fuse32 filter"),
    }

    Ok(())
}

#[test]
fn test_binary_fuse32_decode_unaligned_bytes() -> anyhow::Result<()> {
    let digests = vec![11_u64, 23, 47];
    let mut builder = BinaryFuse32Builder::create();
    builder.add_digests(&digests);
    let filter = builder.build()?;

    let mut bytes = vec![0_u8];
    bytes.extend(filter.to_bytes()?);

    let (decoded, n) = BinaryFuse32Filter::from_bytes(&bytes[1..])?;
    assert_eq!(n, bytes.len() - 1);
    assert_eq!(decoded.len(), Some(3));
    assert!(decoded.contains_digest(23));

    Ok(())
}

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
// limitations under the License.

mod builder;
mod compressed;

use std::io::Cursor;

use chrono_tz::Tz;
use common_clickhouse_srv::binary::*;
use common_clickhouse_srv::errors::Result;
use common_clickhouse_srv::types::*;

#[test]
fn test_write_default() {
    let expected = [1_u8, 0, 2, 255, 255, 255, 255, 0, 0, 0];
    let mut encoder = Encoder::new();
    Block::<Simple>::default().write(&mut encoder, false);
    assert_eq!(encoder.get_buffer_ref(), &expected)
}

#[test]
fn test_compress_block() {
    let expected = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let block = Block::<Simple>::new().column("s", vec!["abc"]);

    let mut encoder = Encoder::new();
    block.write(&mut encoder, true);

    let actual = encoder.get_buffer();
    assert_eq!(actual, expected);
}

#[test]
fn test_decompress_block() {
    let expected = Block::<Simple>::new().column("s", vec!["abc"]);

    let source = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let mut cursor = Cursor::new(&source[..]);
    let actual = Block::load(&mut cursor, Tz::UTC, true).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn test_read_empty_block() {
    let source = [1, 0, 2, 255, 255, 255, 255, 0, 0, 0];
    let mut cursor = Cursor::new(&source[..]);
    match Block::<Simple>::load(&mut cursor, Tz::Zulu, false) {
        Ok(block) => assert!(block.is_empty()),
        Err(_) => unreachable!(),
    }
}

#[test]
fn test_empty() {
    assert!(Block::<Simple>::default().is_empty())
}

#[test]
fn test_column_and_rows() {
    let block = Block::<Simple>::new()
        .column("hello_id", vec![5_u64, 6])
        .column("value", vec!["lol", "zuz"]);

    assert_eq!(block.column_count(), 2);
    assert_eq!(block.row_count(), 2);
}

#[test]
fn test_from_sql() {
    let block = Block::<Simple>::new()
        .column("hello_id", vec![5_u64, 6])
        .column("value", vec!["lol", "zuz"]);

    let v: Result<u64> = block.get(0, "hello_id");
    assert_eq!(v.unwrap(), 5);
}

#[test]
fn test_concat() {
    let block_a = make_block();
    let block_b = make_block();

    let actual = Block::concat(&[block_a, block_b]);
    assert_eq!(actual.row_count(), 4);
    assert_eq!(actual.column_count(), 1);

    assert_eq!(
        "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
        actual.get::<String, _>(0, 0).unwrap()
    );
    assert_eq!(
        "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
        actual.get::<String, _>(1, 0).unwrap()
    );
    assert_eq!(
        "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
        actual.get::<String, _>(2, 0).unwrap()
    );
    assert_eq!(
        "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
        actual.get::<String, _>(3, 0).unwrap()
    );
}

fn make_block() -> Block {
    Block::new().column("9b96ad8b-488a-4fef-8087-8a9ae4800f00", vec![
        "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
        "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
    ])
}

#[test]
fn test_chunks() {
    let first = Block::new().column("A", vec![1, 2]);
    let second = Block::new().column("A", vec![3, 4]);
    let third = Block::new().column("A", vec![5]);

    let block = Block::<Simple>::new().column("A", vec![1, 2, 3, 4, 5]);
    let mut iter = block.chunks(2);

    assert_eq!(Some(first), iter.next());
    assert_eq!(Some(second), iter.next());
    assert_eq!(Some(third), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_chunks_of_empty_block() {
    let block = Block::default();
    assert_eq!(1, block.chunks(100_500).count());
    assert_eq!(Some(block.clone()), block.chunks(100_500).next());
}

#[test]
fn test_rows() {
    let expected = vec![1_u8, 2, 3];
    let block = Block::<Simple>::new().column("A", vec![1_u8, 2, 3]);
    let actual: Vec<u8> = block.rows().map(|row| row.get("A").unwrap()).collect();
    assert_eq!(expected, actual);
}

#[test]
fn test_write_and_read() {
    let block = Block::<Simple>::new().column("y", vec![Some(1_u8), None]);

    let mut encoder = Encoder::new();
    block.write(&mut encoder, false);

    let mut reader = Cursor::new(encoder.get_buffer_ref());
    let rblock = Block::load(&mut reader, Tz::Zulu, false).unwrap();

    assert_eq!(block, rblock);
}

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::binary_write::put_uvarint;

#[test]
fn test_put_uvarint() {
    let expected = [148u8, 145, 6, 0, 0, 0, 0, 0, 0, 0];
    let mut buffer = [0u8; 10];

    let actual = put_uvarint(&mut buffer[..], 100_500);

    assert_eq!(actual, 3);
    assert_eq!(buffer, expected);
}

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

use common_naive_cityhash::cityhash128;
use common_naive_cityhash::U128;

#[test]
fn test_cityhash128() {
    assert_eq!(
        cityhash128("abc".as_ref()),
        U128::new(0x900ff195577748fe, 0x13a9176355b20d7e)
    );
}

#[test]
fn test_from_u128() {
    let v = U128::from(0x11212312341234512345612345671234u128);
    assert_eq!(v.lo, 0x2345612345671234u64);
    assert_eq!(v.hi, 0x1121231234123451u64);
}

#[test]
fn test_into_u128() {
    let v: u128 = U128::new(0x2345612345671234u64, 0x1121231234123451u64).into();
    assert_eq!(v, 0x11212312341234512345612345671234u128);
}

#[test]
fn test_128_len0() {
    assert_eq!(
        cityhash128(b""),
        U128::new(4463240938071824939, 4374473821787594281)
    );
}

#[test]
fn test_128_len1() {
    assert_eq!(
        cityhash128(b"1"),
        U128::new(6359294370932160835, 9352172043616825891)
    );
}

#[test]
fn test_128_len2() {
    assert_eq!(
        cityhash128(b"12"),
        U128::new(16369832005849840265, 11285803613326688650)
    );
}

#[test]
fn test_128_len3() {
    assert_eq!(
        cityhash128(b"123"),
        U128::new(11385344155619871181, 565130349297615695)
    );
}

#[test]
fn test_128_len4() {
    assert_eq!(
        cityhash128(b"1234"),
        U128::new(2764810728135862028, 5901424084875196719)
    );
}

#[test]
fn test_128_len5() {
    assert_eq!(
        cityhash128(b"12345"),
        U128::new(11980518989907363833, 93456746504981291)
    );
}

#[test]
fn test_128_len6() {
    assert_eq!(
        cityhash128(b"123456"),
        U128::new(2350911489181485812, 12095241732236332703)
    );
}

#[test]
fn test_128_len7() {
    assert_eq!(
        cityhash128(b"1234567"),
        U128::new(10270309315532912023, 9823143772454143291)
    );
}

#[test]
fn test_128_len8() {
    assert_eq!(
        cityhash128(b"12345678"),
        U128::new(2123262123519760883, 8251334461883709976)
    );
}

#[test]
fn test_128_len9() {
    assert_eq!(
        cityhash128(b"123456789"),
        U128::new(14140762465907274276, 13893707330375041594)
    );
}

#[test]
fn test_128_len10() {
    assert_eq!(
        cityhash128(b"1234567890"),
        U128::new(8211333661328737896, 17823093577549856754)
    );
}

#[test]
fn test_128_len11() {
    assert_eq!(
        cityhash128(b"1234567890A"),
        U128::new(1841684041954399514, 6623964278873157363)
    );
}

#[test]
fn test_128_len12() {
    assert_eq!(
        cityhash128(b"1234567890Ab"),
        U128::new(3349064628685767173, 12952593207096460945)
    );
}

#[test]
fn test_128_len13() {
    assert_eq!(
        cityhash128(b"1234567890Abc"),
        U128::new(6572961695122645386, 13774858861848724400)
    );
}

#[test]
fn test_128_len14() {
    assert_eq!(
        cityhash128(b"1234567890AbcD"),
        U128::new(18041930573402443112, 5778672772533284640)
    );
}

#[test]
fn test_128_len15() {
    assert_eq!(
        cityhash128(b"1234567890AbcDE"),
        U128::new(11266190325599732773, 348002394938205539)
    );
}

#[test]
fn test_128_len16() {
    assert_eq!(
        cityhash128(b"1234567890AbcDEF"),
        U128::new(15073733098592741404, 5913034415582713572)
    );
}

#[test]
fn test_128_long() {
    assert_eq!(
        cityhash128(b"this is somewhat long string"),
        U128::new(2957911805285034456, 6923665615086076251)
    );
}

#[test]
fn test_128_longer() {
    assert_eq!(
        cityhash128(
            b"DMqhuXQxgAmJ9EOkT1n2lpzu7YD6zKc6ESSDWfJfohaQDwu0ba61bfGMiuS5GXpr0bIVcCtLwRtIVGmK"
        ),
        U128::new(9681404383092874918, 15631953994107571989)
    );
}

#[test]
fn test_128_binary() {
    let data = b"\xe4x\x98\xa4*\xd7\xdc\x02p.\xdeI$\x9fp\xd4\xe3\xd7\xe7L\x86<5h75\xdf0B\x16\xe0\x86\xbeP\xb1rL\x8b\x07\x14!\x9e\xf5\xe0\x9cN\xa5\xfdJ]\xd8J\xc1\xc2.\xe6\xae\x14\xad^sW\x15&";
    assert_eq!(
        cityhash128(data.as_ref()),
        U128::new(5907140908903622203, 10088853506155899265)
    );
}

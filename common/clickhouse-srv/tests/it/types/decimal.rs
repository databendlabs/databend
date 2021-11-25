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

use common_clickhouse_srv::types::*;

#[test]
fn test_new() {
    assert_eq!(Decimal::new(2, 1), Decimal::of(0.2_f64, 1));
    assert_eq!(Decimal::new(2, 5), Decimal::of(0.00002_f64, 5));
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", Decimal::of(2.1_f32, 4)), "2.1000");
    assert_eq!(format!("{}", Decimal::of(0.2_f64, 4)), "0.2000");
    assert_eq!(format!("{}", Decimal::of(2, 4)), "2.0000");
    assert_eq!(format!("{:?}", Decimal::of(2, 4)), "2.0000");
}

#[test]
fn test_eq() {
    assert_eq!(Decimal::of(2.0_f64, 4), Decimal::of(2.0_f64, 4));
    assert_ne!(Decimal::of(3.0_f64, 4), Decimal::of(2.0_f64, 4));

    assert_eq!(Decimal::of(2.0_f64, 4), Decimal::of(2.0_f64, 2));
    assert_ne!(Decimal::of(2.0_f64, 4), Decimal::of(3.0_f64, 2));

    assert_eq!(Decimal::of(2.0_f64, 2), Decimal::of(2.0_f64, 4));
    assert_ne!(Decimal::of(3.0_f64, 2), Decimal::of(2.0_f64, 4));
}

#[test]
fn test_internal32() {
    let internal: i32 = Decimal::of(2, 4).internal();
    assert_eq!(internal, 20000_i32);
}

#[test]
fn test_internal64() {
    let internal: i64 = Decimal::of(2, 4).internal();
    assert_eq!(internal, 20000_i64);
}

#[test]
fn test_scale() {
    assert_eq!(Decimal::of(2, 4).scale(), 4);
}

#[test]
fn test_from_f32() {
    let value: f32 = Decimal::of(2, 4).into();
    assert!((value - 2.0_f32).abs() <= std::f32::EPSILON);
}

#[test]
fn test_from_f64() {
    let value: f64 = Decimal::of(2, 4).into();
    assert!((value - 2.0_f64).abs() < std::f64::EPSILON);
}

#[test]
fn set_scale1() {
    let a = Decimal::of(12, 3);
    let b = a.set_scale(2);

    assert_eq!(2, b.scale());
    assert_eq!(1200, b.internal::<i64>());
}

#[test]
fn set_scale2() {
    let a = Decimal::of(12, 3);
    let b = a.set_scale(4);

    assert_eq!(4, b.scale());
    assert_eq!(120_000, b.internal::<i64>());
}

#[test]
fn test_decimal2str() {
    let d = Decimal::of(0.00001, 5);
    let actual = decimal2str(&d);
    assert_eq!(actual, "0.00001".to_string());
}

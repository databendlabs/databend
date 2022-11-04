// Copyright 2022 Datafuse Labs.
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

use std::mem::MaybeUninit;
use std::num::Wrapping;

use common_expression::types::number::F32;
use common_expression::types::number::F64;
use common_expression::types::NumberType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use once_cell::sync::OnceCell;

const PI: f64 = std::f64::consts::PI;
const PI_F: f32 = std::f32::consts::PI;

const RAD_IN_DEG: f32 = (PI / 180.0) as f32;
const RAD_IN_DEG_HALF: f32 = (PI / 360.0) as f32;

const COS_LUT_SIZE: usize = 1024; // maxerr 0.00063%
const COS_LUT_SIZE_F: f32 = 1024.0f32; // maxerr 0.00063%
const ASIN_SQRT_LUT_SIZE: usize = 512;
const METRIC_LUT_SIZE: usize = 1024;

/// Earth radius in meters using WGS84 authalic radius.
/// We use this value to be consistent with H3 library.
const EARTH_RADIUS: f32 = 6371007.180918475f32;
const EARTH_DIAMETER: f32 = 2f32 * EARTH_RADIUS;

static COS_LUT: OnceCell<[f32; COS_LUT_SIZE + 1]> = OnceCell::new();
static ASIN_SQRT_LUT: OnceCell<[f32; ASIN_SQRT_LUT_SIZE + 1]> = OnceCell::new();

static WGS84_METRIC_METERS_LUT: OnceCell<[f32; 2 * (METRIC_LUT_SIZE + 1)]> = OnceCell::new();

pub fn register(registry: &mut FunctionRegistry) {
    // init globals.
    geo_dist_init();

    // great circle distance
    registry.register_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,NumberType<F32>,_, _>(
        "great_circle_distance",
        FunctionProperty::default(),
        |_,_,_,_|None,
        |lon1:F64,lat1:F64,lon2:F64,lat2:F64,_| {
            F32::from(distance(lon1.0 as f32, lat1.0 as f32, lon2.0 as f32, lat2.0 as f32))
        },
    );
}

pub fn geo_dist_init() {
    // Using `get_or_init` for unit tests cause each test will re-registry all functions.
    COS_LUT.get_or_init(|| {
        let cos_lut: [f32; COS_LUT_SIZE + 1] = (0..=COS_LUT_SIZE)
            .map(|i| (2f64 * PI * i as f64 / COS_LUT_SIZE as f64).cos() as f32)
            .collect::<Vec<f32>>()
            .try_into()
            .unwrap();
        cos_lut
    });

    ASIN_SQRT_LUT.get_or_init(|| {
        let asin_sqrt_lut: [f32; ASIN_SQRT_LUT_SIZE + 1] = (0..=ASIN_SQRT_LUT_SIZE)
            .map(|i| (i as f64 / ASIN_SQRT_LUT_SIZE as f64).sqrt().asin() as f32)
            .collect::<Vec<f32>>()
            .try_into()
            .unwrap();

        asin_sqrt_lut
    });

    WGS84_METRIC_METERS_LUT.get_or_init(|| {
        let mut wgs84_metric_meters_lut: [MaybeUninit<f32>; 2 * (METRIC_LUT_SIZE + 1)] =
            unsafe { MaybeUninit::uninit().assume_init() };

        for i in 0..=METRIC_LUT_SIZE {
            let latitude: f64 = i as f64 * (PI / METRIC_LUT_SIZE as f64) - PI * 0.5f64;

            wgs84_metric_meters_lut[i].write(
                (111132.09f64 - 566.05f64 * (2f64 * latitude).cos()
                    + 1.20f64 * (4f64 * latitude).cos())
                .sqrt() as f32,
            );
            wgs84_metric_meters_lut[i * 2 + 1].write(
                (111415.13f64 * latitude.cos() - 94.55f64 * (3f64 * latitude).cos()
                    + 0.12f64 * (5f64 * latitude).cos())
                .sqrt() as f32,
            );
        }

        // Everything is initialized, transmute and return.
        unsafe {
            std::mem::transmute::<_, [f32; 2 * (METRIC_LUT_SIZE + 1)]>(wgs84_metric_meters_lut)
        }
    });
}

#[inline(always)]
fn geodist_deg_diff(mut f: f32) -> f32 {
    f = f.abs();
    if f > 180f32 {
        f = 360f32 - f;
    }
    f
}

#[inline]
fn geodist_fast_cos(x: f32) -> f32 {
    let mut y = x.abs() * (COS_LUT_SIZE_F / PI_F / 2.0f32);
    let mut i = float_to_index(y);
    y -= i as f32;
    i &= COS_LUT_SIZE - 1;
    let cos_lut = COS_LUT.get().unwrap();
    cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y
}

#[inline]
fn geodist_fast_sin(x: f32) -> f32 {
    let mut y = x.abs() * (COS_LUT_SIZE_F / PI_F / 2.0f32);
    let mut i = float_to_index(y);
    y -= i as f32;
    // cos(x - pi / 2) = sin(x), costable / 4 = pi / 2
    i = (Wrapping(i) - Wrapping(COS_LUT_SIZE / 4)).0 & (COS_LUT_SIZE - 1);
    let cos_lut = COS_LUT.get().unwrap();
    cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y
}

#[inline]
fn geodist_fast_asin_sqrt(x: f32) -> f32 {
    if x < 0.122f32 {
        let x = x as f64;
        let y = x.sqrt();
        return (y
            + x * y * 0.166666666666666f64
            + x * x * y * 0.075f64
            + x * x * x * y * 0.044642857142857f64) as f32;
    }
    if x < 0.948f32 {
        let x = x * ASIN_SQRT_LUT_SIZE as f32;
        let i = float_to_index(x);
        let asin_sqrt_lut = ASIN_SQRT_LUT.get().unwrap();
        return asin_sqrt_lut[i] + (asin_sqrt_lut[i + 1] - asin_sqrt_lut[i]) * (x - i as f32);
    }
    x.sqrt().asin()
}

#[inline(always)]
fn float_to_index(x: f32) -> usize {
    x as usize
}

fn distance(lon1deg: f32, lat1deg: f32, lon2deg: f32, lat2deg: f32) -> f32 {
    let lat_diff = geodist_deg_diff(lat1deg - lat2deg);
    let lon_diff = geodist_deg_diff(lon1deg - lon2deg);

    if lon_diff < 13f32 {
        let latitude_midpoint: f32 = (lat1deg + lat2deg + 180f32) * METRIC_LUT_SIZE as f32 / 360f32;
        let latitude_midpoint_index = float_to_index(latitude_midpoint);

        let wgs84_metric_meters_lut = WGS84_METRIC_METERS_LUT.get().unwrap();
        let k_lat: f32 = wgs84_metric_meters_lut[latitude_midpoint_index * 2]
            + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2]
                - wgs84_metric_meters_lut[latitude_midpoint_index * 2])
                * (latitude_midpoint - latitude_midpoint_index as f32);

        let k_lon: f32 = wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]
            + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2 + 1]
                - wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1])
                * (latitude_midpoint - latitude_midpoint_index as f32);

        (k_lat * lat_diff * lat_diff + k_lon * lon_diff * lon_diff).sqrt()
    } else {
        let a: f32 = (geodist_fast_sin(lat_diff * RAD_IN_DEG_HALF)).powi(2)
            + geodist_fast_cos(lat1deg * RAD_IN_DEG)
                * geodist_fast_cos(lat2deg * RAD_IN_DEG)
                * (geodist_fast_sin(lon_diff * RAD_IN_DEG_HALF)).powi(2);

        EARTH_DIAMETER * geodist_fast_asin_sqrt(a)
    }
}

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

use std::cell::OnceCell;

use common_expression::types::number::F64;
use common_expression::types::NumberType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;

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
const EARTH_DIAMETER: f32 = 2 * EARTH_RADIUS;

static COS_LUT: OnceCell<[f32; COS_LUT_SIZE + 1]> = OnceCell::new();

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,NumberType<F64>,_, _>(
        "great_circle_distance",
        FunctionProperty::default(),
        |_,_,_,_|None,
        |lon1:F64,lat1:F64,lon2:F64,lat2:F64,_| {
            lon1.clone()
        },
    );
}

pub fn geo_dist_init() {
    let cos_lut: [f32; COS_LUT_SIZE + 1] = (0..=COS_LUT_SIZE)
        .map(|i| (((2 * PI * i / COS_LUT_SIZE) as f64).cos()) as f32)
        .collect();
    COS_LUT.set(cos_lut).unwrap(); // todo(ariesdevil): remove unwrap()
}

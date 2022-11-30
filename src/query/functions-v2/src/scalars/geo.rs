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
use std::sync::Arc;
use std::sync::Once;

use common_expression::types::number::Float64Type;
use common_expression::types::number::NumberColumnBuilder;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::F32;
use common_expression::types::number::F64;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;
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
/// We use this value to be consistent with Uber H3 library.
const EARTH_RADIUS: f32 = 6371007.180918475f32;
const EARTH_DIAMETER: f32 = 2f32 * EARTH_RADIUS;

static COS_LUT: OnceCell<[f32; COS_LUT_SIZE + 1]> = OnceCell::new();
static ASIN_SQRT_LUT: OnceCell<[f32; ASIN_SQRT_LUT_SIZE + 1]> = OnceCell::new();

static SPHERE_METRIC_LUT: OnceCell<[f32; METRIC_LUT_SIZE + 1]> = OnceCell::new();
static SPHERE_METRIC_METERS_LUT: OnceCell<[f32; METRIC_LUT_SIZE + 1]> = OnceCell::new();
static WGS84_METRIC_METERS_LUT: OnceCell<[f32; 2 * (METRIC_LUT_SIZE + 1)]> = OnceCell::new();

#[derive(PartialEq)]
enum GeoMethod {
    SphereDegrees,
    SphereMeters,
    Wgs84Meters,
}

struct Ellipse {
    x: f64,
    y: f64,
    a: f64,
    b: f64,
}

pub fn register(registry: &mut FunctionRegistry) {
    // init globals.
    geo_dist_init();

    // geo distance
    registry.register_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,NumberType<F32>,_, _>(
        "geo_distance",
        FunctionProperty::default(),
        |_,_,_,_|FunctionDomain::Full,
        |lon1:F64,lat1:F64,lon2:F64,lat2:F64,_| {
            F32::from(distance(lon1.0 as f32, lat1.0 as f32, lon2.0 as f32, lat2.0 as f32, GeoMethod::Wgs84Meters))
        },
    );

    // great circle angle
    registry.register_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,NumberType<F32>,_, _>(
        "great_circle_angle",
        FunctionProperty::default(),
        |_,_,_,_|FunctionDomain::Full,
        |lon1:F64,lat1:F64,lon2:F64,lat2:F64,_| {
            F32::from(distance(lon1.0 as f32, lat1.0 as f32, lon2.0 as f32, lat2.0 as f32, GeoMethod::SphereDegrees))
        },
    );

    // great circle distance
    registry.register_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,NumberType<F32>,_, _>(
        "great_circle_distance",
        FunctionProperty::default(),
        |_,_,_,_|FunctionDomain::Full,
        |lon1:F64,lat1:F64,lon2:F64,lat2:F64,_| {
            F32::from(distance(lon1.0 as f32, lat1.0 as f32, lon2.0 as f32, lat2.0 as f32, GeoMethod::SphereMeters))
        },
    );

    registry.register_function_factory("point_in_ellipses", |_, args_type| {
        if args_type.len() < 6 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "point_in_ellipses".to_string(),
                args_type: vec![DataType::Number(NumberDataType::Float64); args_type.len()],
                return_type: DataType::Number(NumberDataType::UInt8),
                property: Default::default(),
            },
            calc_domain: Box::new(|_| FunctionDomain::Full),
            eval: Box::new(point_in_ellipses_fn),
        }))
    });
}

fn point_in_ellipses_fn(
    args: &[ValueRef<AnyType>],
    _: EvalContext,
) -> Result<Value<AnyType>, String> {
    let len = args.iter().find_map(|arg| match arg {
        ValueRef::Column(col) => Some(col.len()),
        _ => None,
    });
    let args = args
        .iter()
        .map(|arg| arg.try_downcast::<Float64Type>().unwrap())
        .collect::<Vec<_>>();

    let input_rows = len.unwrap_or(1);

    let ellipses_cnt = (args.len() - 2) / 4;
    let mut ellipses: Vec<Ellipse> = Vec::with_capacity(ellipses_cnt);

    for ellipse_idx in 0..ellipses_cnt {
        let mut ellipse_data = [0.0; 4];
        for (idx, e_data) in ellipse_data.iter_mut().enumerate() {
            let arg_idx = 2 + 4 * ellipse_idx + idx;
            *e_data = match args[arg_idx] {
                ValueRef::Scalar(v) => *v,
                _ => 0f64,
            };
        }
        ellipses.push(Ellipse {
            x: ellipse_data[0],
            y: ellipse_data[1],
            a: ellipse_data[2],
            b: ellipse_data[3],
        });
    }

    let mut start_index = 0;
    let mut builder = NumberColumnBuilder::with_capacity(&NumberDataType::UInt8, input_rows);
    for idx in 0..input_rows {
        let col_x = match &args[0] {
            ValueRef::Scalar(v) => *v,
            ValueRef::Column(c) => unsafe { Float64Type::index_column_unchecked(c, idx) },
        };
        let col_y = match &args[1] {
            ValueRef::Scalar(v) => *v,
            ValueRef::Column(c) => unsafe { Float64Type::index_column_unchecked(c, idx) },
        };

        let r = u8::from(is_point_in_ellipses(
            col_x.0,
            col_y.0,
            &ellipses,
            ellipses_cnt,
            &mut start_index,
        ));
        builder.push(NumberScalar::UInt8(r));
    }

    match len {
        Some(_) => Ok(Value::Column(Column::Number(builder.build()))),
        _ => Ok(Value::Scalar(Scalar::Number(builder.build_scalar()))),
    }
}

fn is_point_in_ellipses(
    x: f64,
    y: f64,
    ellipses: &[Ellipse],
    ellipses_count: usize,
    start_idx: &mut usize,
) -> bool {
    let mut index = *start_idx;
    for _ in 0..ellipses_count {
        let el = &ellipses[index];
        let p1 = (x - el.x) / el.a;
        let p2 = (y - el.y) / el.b;
        if x <= el.x + el.a
            && x >= el.x - el.a
            && y <= el.y + el.b
            && y >= el.y - el.b
            && p1 * p1 + p2 * p2 <= 1.0
        {
            *start_idx = index;
            return true;
        }
        index += 1;
        if index == ellipses_count {
            index = 0;
        }
    }
    false
}

pub fn geo_dist_init() {
    // Using `get_or_init` for unit tests cause each test will re-register all functions.
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

    Once::new().call_once(|| {
        let (wsg84_metric_meters_lut, sphere_metric_meters_lut, sphere_metric_lut) = {
            let mut wgs84_metric_meters_lut: [MaybeUninit<f32>; 2 * (METRIC_LUT_SIZE + 1)] =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut sphere_metric_meters_lut: [MaybeUninit<f32>; METRIC_LUT_SIZE + 1] =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut sphere_metric_lut: [MaybeUninit<f32>; METRIC_LUT_SIZE + 1] =
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

                sphere_metric_meters_lut[i]
                    .write(((EARTH_DIAMETER as f64 * PI / 360f64) * latitude.cos()).powi(2) as f32);

                sphere_metric_lut[i].write(latitude.cos().powi(2) as f32);
            }

            // Everything is initialized, transmute and return.
            unsafe {
                (
                    std::mem::transmute::<_, [f32; 2 * (METRIC_LUT_SIZE + 1)]>(
                        wgs84_metric_meters_lut,
                    ),
                    std::mem::transmute::<_, [f32; METRIC_LUT_SIZE + 1]>(sphere_metric_meters_lut),
                    std::mem::transmute::<_, [f32; METRIC_LUT_SIZE + 1]>(sphere_metric_lut),
                )
            }
        };

        WGS84_METRIC_METERS_LUT.get_or_init(|| wsg84_metric_meters_lut);
        SPHERE_METRIC_METERS_LUT.get_or_init(|| sphere_metric_meters_lut);
        SPHERE_METRIC_LUT.get_or_init(|| sphere_metric_lut);
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

fn distance(lon1deg: f32, lat1deg: f32, lon2deg: f32, lat2deg: f32, method: GeoMethod) -> f32 {
    let lat_diff = geodist_deg_diff(lat1deg - lat2deg);
    let lon_diff = geodist_deg_diff(lon1deg - lon2deg);

    if lon_diff < 13f32 {
        let latitude_midpoint: f32 = (lat1deg + lat2deg + 180f32) * METRIC_LUT_SIZE as f32 / 360f32;
        let latitude_midpoint_index = float_to_index(latitude_midpoint);

        let (k_lat, k_lon) = match method {
            GeoMethod::SphereDegrees => {
                let sphere_metric_lut = SPHERE_METRIC_LUT.get().unwrap();
                let lat = 1f32;
                let lon = sphere_metric_lut[latitude_midpoint_index]
                    + (sphere_metric_lut[latitude_midpoint_index + 1]
                        - sphere_metric_lut[latitude_midpoint_index])
                        * (latitude_midpoint - latitude_midpoint_index as f32);

                (lat, lon)
            }
            GeoMethod::SphereMeters => {
                let sphere_metric_meters_lut = SPHERE_METRIC_METERS_LUT.get().unwrap();
                let lat = (EARTH_DIAMETER * PI_F / 360f32).powi(2);
                let lon = sphere_metric_meters_lut[latitude_midpoint_index]
                    + (sphere_metric_meters_lut[latitude_midpoint_index + 1]
                        - sphere_metric_meters_lut[latitude_midpoint_index])
                        * (latitude_midpoint - latitude_midpoint_index as f32);

                (lat, lon)
            }
            GeoMethod::Wgs84Meters => {
                let wgs84_metric_meters_lut = WGS84_METRIC_METERS_LUT.get().unwrap();
                let lat: f32 = wgs84_metric_meters_lut[latitude_midpoint_index * 2]
                    + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2]
                        - wgs84_metric_meters_lut[latitude_midpoint_index * 2])
                        * (latitude_midpoint - latitude_midpoint_index as f32);

                let lon: f32 = wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]
                    + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2 + 1]
                        - wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1])
                        * (latitude_midpoint - latitude_midpoint_index as f32);

                (lat, lon)
            }
        };

        (k_lat * lat_diff * lat_diff + k_lon * lon_diff * lon_diff).sqrt()
    } else {
        let a: f32 = (geodist_fast_sin(lat_diff * RAD_IN_DEG_HALF)).powi(2)
            + geodist_fast_cos(lat1deg * RAD_IN_DEG)
                * geodist_fast_cos(lat2deg * RAD_IN_DEG)
                * (geodist_fast_sin(lon_diff * RAD_IN_DEG_HALF)).powi(2);

        if method == GeoMethod::SphereDegrees {
            return (360f32 / PI_F) * geodist_fast_asin_sqrt(a);
        }

        EARTH_DIAMETER * geodist_fast_asin_sqrt(a)
    }
}

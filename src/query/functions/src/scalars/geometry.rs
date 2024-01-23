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

use databend_common_expression::types::geometry::GeometryType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use geo::Geometry;
use geo::Point;
use geozero::wkb::Ewkb;
use geozero::CoordDimensions;
use geozero::ToWkb;
use geozero::ToWkt;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType, _, _>(
        "st_makepoint",
        |_,_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType>(|longitude, latitude, builder, ctx| {
            let geom = Geometry::from(Point::new(longitude.0, latitude.0));
            match geom.to_ewkb(CoordDimensions::xy(), None) {
                Ok(data) => {
                    builder.put_slice(data.as_slice())
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string())
                }
            }
            builder.commit_row();
        })
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|b, builder, ctx| {
            match Ewkb(b.to_vec()).to_ewkt(None) {
                Ok(data) => {
                    builder.put_str(&data);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );
}

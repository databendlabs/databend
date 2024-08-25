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

use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_io::geography::geography_from_ewkt;
use databend_common_io::wkb::make_point;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makepoint", &["st_point"]);
    registry.register_aliases("st_geographyfromewkt", &[
        "st_geogfromewkt",
        "st_geographyfromwkt",
        "st_geogfromwkt",
        "st_geographyfromtext",
        "st_geogfromtext",
    ]);

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType, _, _>(
        "st_makepoint",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType> (|lon,lat,builder,ctx|{
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            if let Err(e) = GeographyType::check_point(*lon, *lat) {
                ctx.set_error(builder.len(), e.to_string());
                builder.commit_row()
            } else {
                builder.put_slice(&make_point(*lon, *lat));
                builder.commit_row()
            }
        })
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _, _>(
        "st_geographyfromewkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|wkt, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geography_from_ewkt(wkt) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );
}

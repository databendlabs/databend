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

use databend_common_exception::ErrorCode;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_geo::wkb::make_point;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makepoint", &["st_point"]);

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType, _, _>(
        "st_makepoint",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<NumberType<F64>,NumberType<F64>,GeographyType> (|lon,lat,builder,ctx|{
            if let Err(e) = GeographyType::check_point(*lon, *lat) {
                ctx.set_error(0, ErrorCode::GeometryError(e.to_string()).to_string());
                builder.commit_row()
            } else {
                builder.put_slice(&make_point(*lon, *lat));
                builder.commit_row()
            }
        })
    );
}

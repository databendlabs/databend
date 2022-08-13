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

use common_expression::types::number::Float64Type;
use common_expression::types::number::UInt64Type;
use common_expression::types::number::UInt8Type;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use geo_types::Coordinate;
use h3ron::H3Cell;
use h3ron::Index;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_3_arg::<Float64Type, Float64Type, UInt8Type, UInt64Type, _, _>(
        "geo_to_h3",
        FunctionProperty::default(),
        |_, _, _| None,
        |lon, lat, res| {
            // x must be Longitude and y must be Latitude
            // `h3ron` will transform `Coordinate{x, y}` to `GeoCoord{lat:y, lon:x}` internally.
            let coord = Coordinate { x: lon, y: lat };
            let h3_cell = H3Cell::from_coordinate_unchecked(&coord, res);
            h3_cell.h3index()
        },
    )
}

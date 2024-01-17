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

use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::offset::OffsetsBuffer;

use crate::types::geo::coord::CoordScalar;

pub fn coord_type_to_arrow2_type() -> ArrowDataType {
    let values_field = ArrowField::new("xy", ArrowDataType::Float64, false);
    ArrowDataType::FixedSizeList(Box::new(values_field), 2)
}

pub fn point_data_type() -> ArrowDataType {
    coord_type_to_arrow2_type()
}

pub fn line_string_data_type() -> ArrowDataType {
    let coords_type = coord_type_to_arrow2_type();
    let vertices_field = ArrowField::new("vertices", coords_type, false).into();
    ArrowDataType::LargeList(vertices_field)
}

pub(crate) fn offset_buffer_eq(left: &OffsetsBuffer<i64>, right: &OffsetsBuffer<i64>) -> bool {
    if left.len() != right.len() {
        return false;
    }

    for (o1, o2) in left.iter().zip(right.iter()) {
        if o1 != o2 {
            return false;
        }
    }

    true
}

#[inline]
pub fn coord_eq_allow_nan(left: &CoordScalar, right: &CoordScalar) -> bool {
    // Specifically check for NaN because two points defined to be
    // TODO: in the future add an `is_empty` to the PointTrait and then you shouldn't check for
    // NaN manually
    if left.x().is_nan() && right.x().is_nan() && left.y().is_nan() && right.y().is_nan() {
        return true;
    }

    left == right
}

#[macro_export]
macro_rules! with_geometry_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Point, LineString],
            $($tail)*
        }
    }
}

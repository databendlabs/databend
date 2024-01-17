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

mod coord;
mod geo_trait;
mod linestring;
mod point;
mod utils;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::types::geo::geo_trait::AsArrow;
use crate::types::geo::linestring::LineStringColumn;
use crate::types::geo::linestring::LineStringScalar;
use crate::types::geo::point::PointColumn;
use crate::types::geo::point::PointScalar;
use crate::types::geo::utils::line_string_data_type;
use crate::types::geo::utils::point_data_type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum GeometryDataType {
    Point,
    LineString,
}

impl GeometryDataType {
    pub fn to_arrow2_type(&self) -> ArrowDataType {
        use GeometryDataType::*;
        match self {
            Point => point_data_type(),
            LineString => line_string_data_type(),
        }
    }

    pub fn extension_name(&self) -> &'static str {
        use GeometryDataType::*;
        match self {
            Point => "geoarrow.point",
            LineString => "geoarrow.linestring",
        }
    }
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum GeometryColumn {
    Point(PointColumn),
    LineString(LineStringColumn),
}

impl GeometryColumn {}

impl AsArrow for GeometryColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        match self {
            GeometryColumn::Point(p) => p.as_arrow(arrow_type),
            GeometryColumn::LineString(ls) => ls.as_arrow(arrow_type),
        }
    }
}

// Geometry Scalar
// TODO(ariesdevil): add serde support
#[derive(Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum GeometryScalar<'a> {
    Point(PointScalar),
    LineString(LineStringScalar<'a>),
}

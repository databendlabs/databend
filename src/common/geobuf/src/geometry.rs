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

use geozero::error::GeozeroError;
use ordered_float::OrderedFloat;

use crate::geo_buf;
use crate::geo_buf::Object;
use crate::FeatureKind;
use crate::Geometry;
use crate::ObjectKind;

pub struct BoundingBox {
    pub xmin: f64,
    pub xmax: f64,
    pub ymin: f64,
    pub ymax: f64,
}

impl Geometry {
    pub fn bounding_box(&self) -> BoundingBox {
        fn cmp(a: &f64, b: &f64) -> std::cmp::Ordering {
            std::cmp::Ord::cmp(&OrderedFloat(*a), &OrderedFloat(*b))
        }
        BoundingBox {
            xmin: self
                .column_x
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            xmax: self
                .column_x
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
            ymin: self
                .column_y
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            ymax: self
                .column_y
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
        }
    }

    pub fn memory_size(&self) -> usize {
        self.buf.len() + self.column_x.len() * 16
    }

    pub fn srid(&self) -> Option<i32> {
        if self.buf.len() > 1 {
            self.read_object()
                .map_or(None, |object| Some(object.srid()))
        } else {
            None
        }
    }

    pub(crate) fn read_object(&self) -> Result<Object, GeozeroError> {
        geo_buf::root_as_object(&self.buf[1..]).map_err(|e| GeozeroError::Geometry(e.to_string()))
    }

    pub fn kind(&self) -> Result<ObjectKind, GeozeroError> {
        let kind: FeatureKind = self.buf[0]
            .try_into()
            .map_err(|_| GeozeroError::Geometry("Invalid data".to_string()))?;

        match kind {
            FeatureKind::Geometry(o) | FeatureKind::Feature(o) => Ok(o),
            FeatureKind::FeatureCollection => Ok(ObjectKind::GeometryCollection),
        }
    }
}
